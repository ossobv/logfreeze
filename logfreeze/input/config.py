from collections import namedtuple

from ..tlsconfig import TlsConfig


class InputConfig(namedtuple('InputConfig', (
        'name jetstream tls'))):

    @classmethod
    def from_data(cls, name, data):
        jetstream = data.pop('jetstream')
        if jetstream:
            jetstream = _JetStreamConfig.from_data(jetstream)

        tls = data.pop('tls', None)
        if tls:
            tls = TlsConfig.from_data(tls)

        assert not data, ('leftover data', data)

        return cls(
            name=name,
            jetstream=jetstream,
            tls=tls)


class _JetStreamConfig(namedtuple('JetStreamConfig', (
        'server name subject'))):
    @classmethod
    def from_data(cls, data):
        return cls(**data)
