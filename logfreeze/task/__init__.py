import asyncio

from json import dumps

# from logfreeze.filter import Filter
from logfreeze.input import Producer
from logfreeze.sink import Consumer


def filterforward(inputconfig, sinkconfig, filters):
    async def worker(producer, consumer, filters):
        await producer.connect()
        await consumer.connect()

        try:
            while True:
                try:
                    product = await producer.next()
                except TimeoutError:
                    print('Timeout (no more messages)')
                    continue

                # for filter in filters:
                #     if filter.match(product):

                unit = product.payload['attributes'].get('systemd_unit')
                if unit == 'tetragon-cat.service':  # XXX: hardcoded
                    print(product)
                    section = product.payload['attributes'].get('section')
                    assert section, product
                    subject = subject_tpl.format(
                        section=section, filter='tetragon')
                    # TODO: Do we want to massage the data here? Or something
                    # for later..?
                    await consumer.put(
                        subject, dumps(product.payload).encode('utf-8'))
                # XXX: here we should record the sequence so we can safely
                # restart after this message
        finally:
            try:
                await consumer.close()
            except Exception as e:
                print(f'XXX: consumer close fail: {e}')
            try:
                await producer.close()
            except Exception:
                print(f'XXX: producer close fail: {e}')

    subject_tpl = 'bulk.section.{section}.filter.{filter}'
    assert sinkconfig.jetstream.subject == subject_tpl.format(
        section='*', filter='*'), (sinkconfig.jetstream.subject, subject_tpl)

    p = Producer(inputconfig)
    c = Consumer(sinkconfig)
    asyncio.run(worker(p, c, filters))
