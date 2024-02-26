from collections import namedtuple


class TlsConfig(namedtuple('TlsConfig', (
        'server_name ca_file cert_file key_file'))):

    @classmethod
    def from_data(cls, data):
        server_name = data.pop('server_name', None)
        ca_file = data.pop('ca_file', None)
        cert_file = data.pop('cert_file', None)
        key_file = data.pop('key_file', None)

        if not ca_file:
            ca_file = '/etc/ssl/certs/ca-certificates.crt'
        if cert_file or key_file:
            assert cert_file and key_file, (cert_file, key_file)  # need both

        assert not data, ('leftover data', data)

        return cls(
            server_name=server_name,
            ca_file=ca_file,
            cert_file=cert_file,
            key_file=key_file)
