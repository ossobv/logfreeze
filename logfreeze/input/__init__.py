import asyncio

from json import dumps, loads
from resource import RUSAGE_SELF, getrusage
from time import time

# TODO: Wrap these into something generic?
from nats.errors import ConnectionClosedError, TimeoutError

from .jetstream import Producer


async def purge_subject(producer):
    """
    Print contents and purge from stream.
    """
    await producer.connect()

    i = 0
    while True:
        try:
            product = await producer.next()
        except TimeoutError:
            print('Timeout (no more messages)')
            await producer.close()
            break

        print(i, product.payload)
        await producer.delete(product)

        i += 1


async def log_separate_test(producer):
    """
    Loop over messages, place them in different buckets, count results.
    """
    await producer.connect()

    seen = {}

    r0 = getrusage(RUSAGE_SELF)
    t0 = time()

    def rusage(r0, t0):
        rnow = getrusage(RUSAGE_SELF)
        tnow = time()

        rdelta = (
            'utime', rnow.ru_utime - r0.ru_utime,
            'stime', rnow.ru_stime - r0.ru_stime)
        tdelta = tnow - t0

        print('total wall time', tdelta)
        print('total rusage', rdelta)
        print(rnow)

    i = 0
    while True:
        try:
            # Test run over 50.000 records
            # ----------------------------
            # total wall time 4.591595411300659
            # total rusage ('utime', 3.657911, 'stime', 0.136071)
            # struct_rusage(ru_maxrss=45464, ru_minflt=9729, ru_majflt=0)
            product = await producer.next_through_fetch(100)
            # total wall time 37.27816319465637
            # total rusage ('utime', 13.668639, 'stime', 1.116393)
            # struct_rusage(ru_maxrss=43556, ru_minflt=8230, ru_majflt=0)
            product = await producer.next_through_getmsg()
        except TimeoutError:
            print('Timeout (no more messages)')
            await producer.close()
            break

        uniq = ()

        filename = product.payload['attributes'].get('filename')
        job = product.payload['attributes'].get('job')
        unit = product.payload['attributes'].get('systemd_unit')

        try:
            headers = product.payload
            body = headers.pop('message')

            if filename in (
                    '/var/log/auth.log', '/var/log/audit/audit.log'):
                uniq = f'filename={filename}'

            elif job == 'loki.source.journal.logs_journald_generic':
                body = loads(body)

                if body['_TRANSPORT'] == 'audit':
                    uniq = f'job={job} _TRANSPORT={body["_TRANSPORT"]}'

                elif (body['_TRANSPORT'] == 'syslog' and
                        body['MESSAGE'].startswith((
                            'pam_unix(sudo:session): session opened for user ',
                            'pam_unix(sudo:session): session closed for user '
                        ))):
                    uniq = (
                        f'job={job} _TRANSPORT={body["_TRANSPORT"]} '
                        f'MESSAGE.startswith="pam_unix(sudo:session): '
                        f'session [opened|closed] for user "')

                elif (body['_TRANSPORT'] == 'syslog' and
                        body['SYSLOG_IDENTIFIER'] == 'sudo' and
                        ' COMMAND=' in body['MESSAGE']):
                    uniq = (
                        f'job={job} _TRANSPORT={body["_TRANSPORT"]} '
                        f'SYSLOG_IDENTIFIER={body["SYSLOG_IDENTIFIER"]} '
                        f'MESSAGE.contains=" COMMAND="')

                elif body.get('_EXE') in (
                        '/usr/bin/sudo', '/usr/sbin/cron', '/usr/sbin/sshd'):
                    uniq = f'job={job} _EXE={body["_EXE"]}'

                elif (body.get('_COMM') == 'sshd' and
                        body['SYSLOG_IDENTIFIER'] == 'sshd'):
                    uniq = (
                        f'job={job} _COMM={body["_COMM"]} '
                        f'SYSLOG_IDENTIFIER={body["SYSLOG_IDENTIFIER"]}')

                elif unit in ('cron.service', 'systemd-logind.service'):
                    uniq = f'job={job} systemd_unit={unit}'

            elif job == 'loki.source.journal.logs_journald_grafana_agent_flow':
                uniq = f'job={job}'

        except KeyError:
            print('>>>', product.payload, '<<<')
            raise

        if not uniq:
            print('[\x1b[31;1m', i, '\x1b[0m]', dumps(headers))
            print(dumps(body))
            print()
        elif uniq in seen:
            seen[uniq]['count'] += 1
        elif uniq not in seen:
            seen[uniq] = {
                'count': 1,
                'headers': dumps(headers),
                'body': dumps(body),
            }
            print('[\x1b[32;1m', i, '\x1b[0m]', dumps(headers))
            print(dumps(body))
            print()

        i += 1
        if (i % 100_000) == 0:
            print(i)
        if i == 50_000:
            rusage(r0, t0)
            break

    try:
        await producer.close()
    except ConnectionClosedError:
        pass

    print()
    print('=' * 72)
    print()

    for key, data in sorted(seen.items()):
        print('[\x1b[33;1m', data['count'], '\x1b[0m]\x1b[1m', key, '\x1b[0m')
        print(data['headers'])
        print(data['body'])
        print()


def test_input(inputconfig):
    p = Producer(inputconfig)
    # #asyncio.run(purge_subject(p))
    asyncio.run(log_separate_test(p))
