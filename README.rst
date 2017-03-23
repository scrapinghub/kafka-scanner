High Level Kafka Scanner
========================

Features:

* based on `kafka-python <https://github.com/mumrah/kafka-python/commits/v1.3.1>`_ library
* reverse reading of a kafka topic in batches
* deduplication by key
* provides fake kafka-python consumer/client for mocking when testing code that uses this library classes

Two classes are provided:

* `KafkaScanner` - reverse scan feature. Because the particular usage of the inverse logic, this class doesn't
                   commit offsets (and so doesn't support consumer group). It always start from the latest offsets
                   down to the lowest offsets.
* `KafkaScannerDirect` - direct scan.

Check classes docstrings for parameters and more information


Basic example
=============

.. code:: python
    
    from kafka_scanner import KafkaScanner
    KAFKA_BROKERS = ['kafka1.example.com:9092', 'kafka2.example.com:9092', 'kafka3.example.com:9092']

    scanner = KafkaScanner(KAFKA_BROKERS, <topic name>, partitions=[<num partition>])
    batches = scanner.scan_topic_batches()
    for b in batches:
        for m in b:
            do_my_thing(m)


SSL example
=============

Set the ssl configs in a dict `ssl_configs` and pass it to the scanner constructor.

.. code:: python

    from kafka_scanner import KafkaScanner
    KAFKA_BROKERS = ['kafka1.example.com:9093', 'kafka2.example.com:9093', 'kafka3.example.com:9093']

    ssl_configs = {
        'ssl_cafile': '/path/to/ca.crt',
        'ssl_certfile': '/path/to/client.crt',
        'ssl_keyfile': '/path/to/client.key',
    }
    scanner = KafkaScanner(KAFKA_BROKERS, <topic name>, partitions=[<num partition>], ssl_configs=ssl_configs)
    ...
