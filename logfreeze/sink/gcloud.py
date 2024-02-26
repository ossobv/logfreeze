from json import loads

from gcloud import storage
from google.oauth2 import service_account


def get_google_application_credentials():
    # Define your service account credentials as a dictionary.
    credentials_dict = {
        'type': 'service_account',
        'project_id': 'your-project-id',
        'private_key_id': 'your-private-key-id',
        'private_key': (
            '-----BEGIN PRIVATE KEY-----\nYourPrivateKey\n'
            '-----END PRIVATE KEY-----\n'),
        'client_email': 'your-client-email',
        'client_id': 'your-client-id',
        'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
        'token_uri': 'https://accounts.google.com/o/oauth2/token',
        'auth_provider_x509_cert_url': (
            'https://www.googleapis.com/oauth2/v1/certs'),
        'client_x509_cert_url': 'your-client-cert-url'
    }

    with open(
            './logfreeze-service-account-file.json'  # FIXME
            ) as fp:
        credentials_dict = loads(fp.read())

    return service_account.Credentials.from_service_account_info(
        credentials_dict)


# Authenticate with Google Cloud Storage
def authenticate_with_gcs():
    # Create a client using default credentials
    try:
        client = storage.Client(
            credentials=get_google_application_credentials())
    except Exception as e:
        print(f"Authentication failed: {e}")
        return None
    else:
        print("Authenticated successfully!")
        return client


# # Instantiates a client
# storage_client = storage.Client()
#
# # The name for the new bucket
# bucket_name = "my-new-bucket"
#
# # Creates the new bucket
# bucket = storage_client.create_bucket(bucket_name)
#
# print(f"Bucket {bucket.name} created.")
#
#    Client(
#            project=<object object>, credentials=None, _http=None,
#            client_info=None, client_options=None,
#            use_auth_w_custom_endpoint=True, extra_headers={})
#    client = storage.Client(
#        project='my_project', credentials='
#    bucket = client.get_bucket('bucket-id-here')
#    # Then do other things...
#    blob = bucket.get_blob('remote/path/to/file.txt')
#    print blob.download_as_string()
#    blob.upload_from_string('New contents!')
#    blob2 = bucket.blob('remote/path/storage.txt')
#    blob2.upload_from_filename(filename='/local/path.txt')
