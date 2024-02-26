from .gcloud import authenticate_with_gcs


def test_sink():
    authenticate_with_gcs()
