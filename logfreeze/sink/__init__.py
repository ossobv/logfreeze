# TODO: This should be wrapped into something generic.
from .gcloud import list_buckets
# XXX: We can't all call them consumers though..
from .jetstream import Consumer


def test_connect(sinkconfig):
    list_buckets(sinkconfig)
