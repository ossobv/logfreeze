from collections import namedtuple
from datetime import datetime
from json import loads
from ssl import Purpose, create_default_context

from nats import connect
from nats.errors import TimeoutError
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy
from nats.js.manager import JetStreamManager


if 0:
    def monkey_patch_nats_tcptransport_for_tracing():
        from sys import stderr
        from nats.aio.transport import TcpTransport as class_

        def replace(cls, prop, func):
            setattr(cls, f'orig_{prop}', getattr(cls, prop))
            setattr(cls, prop, func)

        def new_write(self, payload):
            print('>>> (w)', payload, file=stderr)
            return self.orig_write(payload)

        def new_writelines(self, payload):
            print('>>> (W)', payload, file=stderr)
            return self.orig_writelines(payload)

        async def new_read(self, *args, **kwargs):
            ret = await self.orig_read(*args, **kwargs)
            print('<<< (r)', ret, file=stderr)
            return ret

        async def new_readline(self, *args, **kwargs):
            ret = await self.orig_readline(*args, **kwargs)
            print('<<< (R)', ret, file=stderr)
            return ret

        replace(class_, 'write', new_write)
        replace(class_, 'writelines', new_writelines)
        replace(class_, 'read', new_read)
        replace(class_, 'readline', new_readline)

    monkey_patch_nats_tcptransport_for_tracing()


class Product(namedtuple('Product', 'payload rseq timestamp')):
    def __new__(cls, *args, **kwargs):
        if 'timestamp' not in kwargs:
            attr = kwargs['payload']['attributes']
            if 'time_unix_nano' in attr:
                # 1708943566883520207
                ts = attr['time_unix_nano'] / 1e9
                kwargs['timestamp'] = datetime.utcfromtimestamp(ts)
            else:
                # "2024-02-15T12:08:01.572264746Z"
                ts = kwargs['payload']['timestamp']
                if len(ts) == 30 and ts.endswith('Z'):
                    ts = ts[0:26]  # drop "...Z" so we have microseconds
                    fmt = '%Y-%m-%dT%H:%M:%S.%f'
                elif len(ts) == 27 and ts.endswith('Z'):
                    fmt = '%Y-%m-%dT%H:%M:%S.%fZ'
                elif len(ts) == 24 and ts.endswith('Z'):
                    fmt = '%Y-%m-%dT%H:%M:%S.%fZ'
                else:
                    raise NotImplementedError(ts)
                kwargs['timestamp'] = datetime.strptime(ts, fmt)

        return super().__new__(cls, *args, **kwargs)


class Producer:
    def __init__(self, inputconfig):
        self.inputconfig = inputconfig
        # FIXME: Come up with a better name for the durable consumer.
        if 1:
            self.durable_name = f'logfreeze_{inputconfig.name}'
        else:
            self.durable_name = None  # ephemeral
        self._natsc = None
        self._jsmgr = None
        self._sub = None
        self._messages = []
        # #await nc.close()

    async def connect(self):
        if self.inputconfig.tls:
            tls_ctx = create_default_context(purpose=Purpose.SERVER_AUTH)
            tls_ctx.load_verify_locations(self.inputconfig.tls.ca_file)

            if self.inputconfig.tls.cert_file:
                tls_ctx.load_cert_chain(
                    certfile=self.inputconfig.tls.cert_file,
                    keyfile=self.inputconfig.tls.key_file)

            tls_hostname = self.inputconfig.tls.server_name
        else:
            tls_ctx = None
            tls_hostname = None

        assert not self._natsc, self._natsc
        natsc = await connect(
            servers=[self.inputconfig.jetstream.server],
            tls=tls_ctx, tls_hostname=tls_hostname,
            name='logfreeze.input',  # FIXME: more naming..
            verbose=False)

        jsmgr = JetStreamManager(natsc)
        stream_name = await jsmgr.find_stream_name_by_subject(
            self.inputconfig.jetstream.subject)
        assert stream_name == self.inputconfig.jetstream.name, (
            stream_name, 'but expected', self.inputconfig.jetstream.name)
        js = natsc.jetstream()

        if self.durable_name:
            # Setting deliver_subject to None will cause [the] consumer to be
            # "pull-based", and will require explicit acknowledgment of each
            # message. This is analogous in some ways to a normal NATS queue
            # subscriber, where a message will be delivered to a single
            # subscriber. Pull-based consumers are intended to be used for
            # workloads where it is desirable to have a single process receive
            # a message.
            # https://docs.rs/async-nats/latest/async_nats/jetstream/consumer/struct.Config.html
            assert '.' not in self.durable_name
            assert '*' not in self.durable_name
            assert '>' not in self.durable_name
            config = ConsumerConfig(
                durable_name=self.durable_name,
                deliver_policy=DeliverPolicy.BY_START_SEQUENCE,
                opt_start_seq=1,
                ack_policy=AckPolicy.EXPLICIT)
            info = await jsmgr.add_consumer(
                stream=self.inputconfig.jetstream.name,
                config=config)
            sub = await js.pull_subscribe(
                subject=self.inputconfig.jetstream.subject,
                # queue= <-- if we wanted to split the load.. (horiz scale)
                stream=self.inputconfig.jetstream.name,
                durable=self.durable_name,
                pending_bytes_limit=0,
                pending_msgs_limit=0)

        else:
            sub = await js.pull_subscribe(
                subject=self.inputconfig.jetstream.subject,
                stream=self.inputconfig.jetstream.name)
            info = await sub.consumer_info()

        self._stream_name = info.stream_name
        # #self._stream_name = self.inputconfig.jetstream.name

        print('INPUT', self.inputconfig.name, info)
        print()
        print('SUB', sub)
        print()

        self._natsc = natsc
        self._jsmgr = jsmgr
        self._sub = sub
        self._next_seq = 1  # do NOT start at zero! messes up JS internals

    async def close(self):
        await self._natsc.flush()
        await self._natsc.close()

        self._natsc = self._jsmgr = self._sub = self._next_seq = None

    async def next(self):
        return await self.next_through_fetch(100)

    async def next_through_fetch(self, batch_size=10):
        """
        This one is fast: it starts the stream at 1 for ephemeral consumers,
        but for durable ones, we start wherever we left off.
        """
        try:
            msg = self._messages.pop(0)
        except IndexError:
            try:
                msgs = await self._sub.fetch(batch=batch_size, timeout=3)
            except TimeoutError as e:
                raise TimeoutError from e
            else:
                assert 1 <= len(msgs) <= batch_size, (batch_size, msgs)
                self._messages = msgs
            msg = self._messages.pop(0)

        product = Product(
            payload=loads(msg.data),
            # Remote Sequence: this is also the ID used when deleting a remote
            # messages.
            rseq=msg.metadata.sequence.stream,
            # Store ref to internal message.
            # #internal=msg,
            # NOTE: This is a jetstream timestamp, not the original one.
            timestamp=msg.metadata.timestamp)

        await msg.ack()
        return product

    async def next_through_getmsg(self):
        """
        This one is slow but it can start/resume the stream anywhere.

        Unsure whether direct=False or True is better. It appears as though
        direct=False is slightly faster.
        """
        try:
            msg = await self._jsmgr.get_msg(
                stream_name=self._stream_name,
                seq=self._next_seq,
                subject=self.inputconfig.jetstream.subject,
                # Allow from replica can only be True if --allow-direct is
                # enabled on the stream. When it is, we could get batched
                # requests, but only if the nats server has:
                # https://github.com/nats-io/nats-server/commit/ca4ddb62caf6eb6c0c4c5d6efd894bd9d1d4701e
                # For direct=False, the batched requests are not supported.
                direct=False,
                next=True)      # give the next message >= seq
        except TimeoutError as e:
            raise TimeoutError from e

        product = Product(
            payload=loads(msg.data),
            # #internal=msg
            rseq=msg.seq)

        # NOTE: Cannot ack get_msg() provided messages.
        assert msg.seq >= self._next_seq, (msg.seq, self._next_seq)
        self._next_seq = (msg.seq + 1)
        return product

    async def delete(self, product):
        # seq = product.internal.metadata.sequence
        seq = product.rseq
        # seq.consumer is not the remote id, which is seq.stream
        ret = await self._jsmgr.delete_msg(self._stream_name, seq.stream)
        assert ret is True, ('delete fail?', ret, self._stream_name, seq)
