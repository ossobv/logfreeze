from collections import namedtuple

from ..tlsconfig import TlsConfig


class SinkConfig(namedtuple('SinkConfig', (
        'name gcloud jetstream tls'))):

    @classmethod
    def from_data(cls, name, data):
        gcloud = data.pop('gcloud', None)
        if gcloud:
            gcloud = cls._validate_gcloud(gcloud)

        jetstream = data.pop('jetstream', None)
        if jetstream:
            jetstream = _JetStreamConfig.from_data(jetstream)

        tls = data.pop('tls', None)
        if tls:
            tls = TlsConfig.from_data(tls)

        assert not data, ('leftover data', data)

        assert sum(int(bool(backend)) for backend in (gcloud, jetstream)) == 1

        # FIXME: We might want a separate Config for different sink types and
        # just use SinkConfig as a ConfigFactory.
        return cls(
            name=name,
            gcloud=gcloud,
            jetstream=jetstream,
            tls=tls)

    @classmethod
    def _validate_gcloud(cls, gcloud):
        keys = set(gcloud.keys())
        expected_keys = set([
            'type', 'project_id', 'private_key_id', 'private_key',
            'client_email', 'client_id', 'auth_uri', 'token_uri',
            'auth_provider_x509_cert_url', 'client_x509_cert_url',
            'universe_domain'])
        assert keys == expected_keys, (
            'gcloud config bad',
            (keys - expected_keys), '<- excess',
            (expected_keys - keys), '<- missing')
        return gcloud

    def __repr__(self):
        if self.gcloud:
            return (
                f"SinkConfig(name={self.name!r}, "
                f"gcloud={{'type': {self.gcloud['type']!r}, "
                f"'project_id': {self.gcloud['project_id']!r}, ...}})")
        return super().__repr__()


class _JetStreamConfig(namedtuple('JetStreamConfig', (
        'server name subject'))):
    @classmethod
    def from_data(cls, data):
        return cls(**data)
