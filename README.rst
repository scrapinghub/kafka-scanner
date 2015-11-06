High Level Kafka Scanner
========================

Features:

* based on `kafka-python <https://github.com/mumrah/kafka-python/commits/v0.9.4>`_ library
* reverse reading of a kafka topic in blocks
* deduplication by key
* faster access to messages by key prefix, based on assumption that messages are clusterized by key prefix
* provides fake kafka-python consumer/client for mocking when testing code that uses this library classes

Basic example
=============

.. code:: python
    
    from kafka_scanner import KafkaScannerSimple
    KAFKA_BROKERS = ['kafka1.example.com:9092', 'kafka2.example.com:9092', 'kafka3.example.com:9092']

    scanner = KafkaScannerSimple(KAFKA_BROKERS, <topic name>, partitions=[<num partition>])
    batches = scanner.scan_topic_batches()
    for b in batches:
        for m in b:
            do_my_thing(m)

Check class docstring for more options.
