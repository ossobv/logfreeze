logfreeze
=========

*Function one:* **replicating a JetStream to remote storage (logfreeze).**

*Function two:* **filtering and distributing logs to designated JetStreams**

For both cases we want a "pull consumer" because we'll always want to be
able to run late. Never should messages be dropped just because we
weren't there to listen.

A durable (named) consumer provides such a thing. Hopefully it is
ordered as well.


----
TODO
----

☐  Make the JetStream consumers durable and test their speed.

☐  Create a PoC for filtering tetragon audit logs and republishing them to the next stream.

☐  Add JetStream target/sink configuration?

☐  The rest.. like readable filter rules, filter groups, filter examples.

☐  Redo PoC in Rust when speed is of the essence.


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

    [sink.my_gcloud]
    gcloud.type = 'service_account'
    gcloud.project_id = 'my-project-1234'
    gcloud.private_key_id = '0123456789abcdef0123456789abcdef01234567'
    gcloud.private_key = "-----BEGIN PRIVATE KEY-----\n...-----END PRIVATE KEY-----\n"
    gcloud.client_email = '...@...iam.gserviceaccount.com'
    gcloud.client_id = '12345'
    gcloud.auth_uri = 'https://accounts.google.com/o/oauth2/auth'
    gcloud.token_uri = 'https://oauth2.googleapis.com/token'
    gcloud.auth_provider_x509_cert_url = 'https://www.googleapis.com/oauth2/v1/certs'
    gcloud.client_x509_cert_url = 'https://www.googleapis.com/robot/v1/...'
    gcloud.universe_domain = 'googleapis.com'
