from sys import stderr

from google.auth import load_credentials_from_dict
from google.cloud import storage


def list_buckets(sinkconfig):
    print(f'Attempting auth on {sinkconfig!r}...')
    client = authenticate_to_gcloud(sinkconfig)
    if not client:
        return

    print('Attempting bucket list...')
    print([b for b in client.list_buckets()])
    client.create_bucket('testing123')
    # bucket = client.get_bucket('bucket-id-here')
    # blob = bucket.get_blob('remote/path/to/file.txt')
    # print blob.download_as_string()
    # blob.upload_from_string('New contents!')
    # blob2 = bucket.blob('remote/path/storage.txt')
    # blob2.upload_from_filename(filename='/local/path.txt')


def authenticate_to_gcloud(sinkconfig):
    credentials, project = load_credentials_from_dict(sinkconfig.gcloud)
    # credentials = credentials.with_scopes(  # ???
    #     ['https://www.googleapis.com/auth/cloud-platform'])
    try:
        client = storage.Client(
            project=project,
            credentials=credentials)
    except Exception as e:
        print(f'Authentication failed: {e}', file=stderr)
        return None
    return client
