from ssl import Purpose, create_default_context

from nats import connect


class Consumer:
    def __init__(self, sinkconfig):
        self.sinkconfig = sinkconfig
        self._natsc = None
        self._js = None
        # #await nc.close()

    async def connect(self):
        if self.sinkconfig.tls:
            tls_ctx = create_default_context(purpose=Purpose.SERVER_AUTH)
            tls_ctx.load_verify_locations(self.sinkconfig.tls.ca_file)

            if self.sinkconfig.tls.cert_file:
                tls_ctx.load_cert_chain(
                    certfile=self.sinkconfig.tls.cert_file,
                    keyfile=self.sinkconfig.tls.key_file)

            tls_hostname = self.sinkconfig.tls.server_name
        else:
            tls_ctx = None
            tls_hostname = None

        assert not self._natsc, self._natsc
        natsc = await connect(
            servers=[self.sinkconfig.jetstream.server],
            tls=tls_ctx, tls_hostname=tls_hostname,
            name='logfreeze.sink',  # FIXME: naming..
            verbose=False)
        js = natsc.jetstream()

        # TODO: add stream options here..
        await js.add_stream(
            name=self.sinkconfig.jetstream.name,
            subjects=[self.sinkconfig.jetstream.subject])

        self._natsc = natsc
        self._js = js

    async def close(self):
        await self._natsc.flush()
        await self._natsc.close()

        self._natsc = self._js = None

    async def put(self, subject, data):
        ack = await self._js.publish(subject, data)
        del ack
