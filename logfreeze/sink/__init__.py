# TODO: This should be wrapped into something generic.
from .gcloud import list_buckets


def test_connect(sinkconfig):
    list_buckets(sinkconfig)
