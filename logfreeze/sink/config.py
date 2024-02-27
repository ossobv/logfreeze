from collections import namedtuple


class SinkConfig(namedtuple('SinkConfig', (
        'name gcloud'))):

    @classmethod
    def from_data(cls, name, data):
        gcloud = data.pop('gcloud', None)
        assert gcloud, 'only gcloud config for now..'

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

        # FIXME: We might want a separate Config for different sink types and
        # just use SinkConfig as a ConfigFactory.
        return cls(name=name, gcloud=gcloud)

    def __repr__(self):
        return (
            f"SinkConfig(name={self.name!r}, "
            f"gcloud={{'type': {self.gcloud['type']!r}, "
            f"'project_id': {self.gcloud['project_id']!r}, ...}})")
