High Level Kafka Scanner
========================

Features:

* based on `kafka-python <https://github.com/mumrah/kafka-python/commits/v0.9.4>`_ library
* reverse reading of a kafka topic in blocks
* deduplication by key
* faster access to messages by kafka prefix, based on assumption that messages are clusterized by key prefix
* provides fake kafka-python consumer/client for mocking when testing code that uses this library classes
