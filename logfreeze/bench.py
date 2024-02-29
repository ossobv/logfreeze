from contextlib import contextmanager
from resource import RUSAGE_SELF, getrusage
from sys import stderr
from time import time


@contextmanager
def measure_resources(title):
    r0 = getrusage(RUSAGE_SELF)
    t0 = time()

    try:
        yield  # (r0, t0)
    finally:
        rnow = getrusage(RUSAGE_SELF)
        tnow = time()

        delta = {
            'real': tnow - t0,
            'utime': rnow.ru_utime - r0.ru_utime,
            'stime': rnow.ru_stime - r0.ru_stime
        }
        print(f'{title}: {delta}', file=stderr)
