from json import dumps, loads

import asyncio

# TODO: Wrap these into something generic?
from nats.errors import ConnectionClosedError, TimeoutError

from logfreeze.bench import measure_resources

from .jetstream import Producer


def test_connect(inputconfig):
    async def main(producer):
        await producer.connect()
        await producer.close()

    p = Producer(inputconfig)
    asyncio.run(main(p))


def test_timings(inputconfig):
    async def main(producer):
        # next_through_fetch
        await p.connect()
        with measure_resources('50.000 fetches with batch 100'):
            for i in range(50000):
                product = await producer.next_through_fetch(100)
        await producer.close()
        print(product)
        print()

        # next_through_getmsg
        await p.connect()
        with measure_resources('50.000 fetches with getmsg'):
            for i in range(50000):
                product = await producer.next_through_getmsg()
        await producer.close()
        print(product)
        print()

    p = Producer(inputconfig)
    asyncio.run(main(p))


def test_dev(inputconfig):
    async def main(producer):
        # #await example_purge_subject(producer)
        await log_separate_test(producer)

    p = Producer(inputconfig)
    asyncio.run(main(p))


async def log_separate_test(producer):
    """
    Loop over messages, place them in different buckets, count results.
    """
    await producer.connect()

    seen = {}

    i = 0
    while True:
        try:
            product = await producer.next()
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


async def example_purge_subject(producer):
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
