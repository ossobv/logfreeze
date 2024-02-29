from collections import namedtuple
from datetime import datetime
from json import loads
from ssl import Purpose, create_default_context

from nats import connect
from nats.errors import TimeoutError
from nats.js.manager import JetStreamManager


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


class Timeout(Exception):
    pass


class Producer:
    def __init__(self, inputconfig):
        self.inputconfig = inputconfig
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
            servers=[self.inputconfig.jetstream_server],
            tls=tls_ctx, tls_hostname=tls_hostname)

        jsmgr = JetStreamManager(natsc)
        stream_name = await jsmgr.find_stream_name_by_subject(
            self.inputconfig.jetstream_subject)
        assert stream_name == self.inputconfig.jetstream_name, (
            stream_name, 'but expected', self.inputconfig.jetstream_name)

        js = natsc.jetstream()
        sub = await js.pull_subscribe(
            subject=self.inputconfig.jetstream_subject,
            stream=self.inputconfig.jetstream_name)

        # XXX: no possibility to configure the Consumer?
        # See:
        # https://docs.rs/async-nats/latest/async_nats/jetstream/consumer/struct.Config.html
        # sub = await js.pull_subscribe(
        #     durable='logfreeze',
        #     subject='bulk.section.*')

        info = await sub.consumer_info()
        self._stream_name = info.stream_name
        # #self._stream_name = self.inputconfig.jetstream_name

        print('INPUT', self.inputconfig.name, info)
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
        # Fastest.. until we have a working next_through_consumer.
        return await self.next_through_fetch(100)

    async def next_through_consumer(self, batch_size=10):
        """
        This one is fast (??? is it?) and it has NATS do bookkeeping of the
        stream position.
        """
        raise NotImplementedError()

    async def next_through_fetch(self, batch_size=10):
        """
        This one is fast but it always starts the stream at 1.
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
                subject=self.inputconfig.jetstream_subject,
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
