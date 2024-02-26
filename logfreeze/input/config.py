from collections import namedtuple

from ..tlsconfig import TlsConfig


class InputConfig(namedtuple('InputConfig', (
        'name jetstream_server jetstream_name jetstream_subject tls'))):

    @classmethod
    def from_data(cls, name, data):
        jetstream = data.pop('jetstream')
        jetstream_server = jetstream.pop('server')
        jetstream_name = jetstream.pop('name')  # not necessary??
        jetstream_subject = jetstream.pop('subject')

        tls = data.pop('tls', None)
        if tls:
            tls = TlsConfig.from_data(tls)

        assert not data, ('leftover data', data)

        return cls(
            name=name,
            jetstream_server=jetstream_server,
            jetstream_name=jetstream_name,
            jetstream_subject=jetstream_subject,
            tls=tls)
