logfreeze
=========

Functions, *to be defined.*


----
TODO
----

- pull_subscription offset? start at stream_id X. This seems not to be
  implemented easily. One can use ``get_msg()``, but it is significantly
  slower than ``fetch(100)``.

  4-5 secs for ``fetch(100)`` vs. 30-50 secs for ``get_msg()`` with a
  sequence id.

- Readable filters.

- Filter groupings..

- Filter examples?


-----------
Config file
-----------

.. code-block:: toml

    [input.my_jetstream]
    # Input source
    jetstream.server = 'nats://10.20.30.40:4222'
    jetstream.name = 'my_stream'
    #jetstream.subject = 'bulk.section.abc'
    jetstream.subject = 'bulk.section.def'
    # Server certificate validation
    #tls.server_name = 'server_name_for_cert_validation'
    tls.ca_file = './nats_ca.crt'
    # Client certificate
    tls.cert_file = './nats_client.crt'
    tls.key_file = './nats_client.key'
