# coding=utf-8
"""
Tools for mocking kafka-python objects in order to allow to build tests
"""

import zlib
from collections import namedtuple
from itertools import cycle

import msgpack


Message = namedtuple("Message", ["key", "value"])
ConsumerRecord = namedtuple("ConsumerRecord", ["partition", "offset", "key", "value"])


def get_kafka_msg_samples(msgs=None, fetch_count=0):
    if not msgs:
        msgs = [('AD12345', "my-body"),
                ('AD34567', "second my-body"),
                ('AD67890', 'third my-body')]
    fetch_count = fetch_count or len(msgs)
    return [Message(key,
         zlib.compress(msgpack.packb({'body': body})) if body else None)
         for key, body in msgs[:fetch_count]]


class FakeConsumer(object):
    def __init__(self, client, mock=None, fail_on_offset=None):
        self.client = client
        self.count_since_commit = 0
        self._offsets = None
        self.record_queue = []
        self.mock = mock
        self.fail_on_offset = fail_on_offset

    def provide_partition_info(self):
        self.partition_info = True

    def get_records(self, count=1, *args, **kwargs):
        purged_partitions = []
        while self.record_queue:
            record = self.record_queue[0]
            if record.partition in purged_partitions:
                break
            if record.offset != self.offsets[record.partition]:
                self.record_queue.pop(0)
            else:
                purged_partitions.append(record.partition)
        records = []
        while count:
            if not self.record_queue:
                for record in self.client.get_msg_generator(count, self.offsets):
                    self.record_queue.append(record)
            if self.record_queue:
                record = self.record_queue.pop(0)
                assert (self.fail_on_offset != record.offset), 'Failed on offset {}'.format(record.offset)
                self.offsets[record.partition] = record.offset + 1
                records.append(record)
                count -= 1
            else:
                break
        return records

    def seek(self, offset, whence=None, partition=None):
        partitions = self.offsets.keys() if partition is None else [partition]
        for p in partitions:
            if whence is None:
                self.offsets[p] = offset
            elif whence == 1:
                self.offsets[p] += offset

    def commit(self):
        if self.count_since_commit > 0:
            self.client.offsets.update(self.offsets)
            self.count_since_commit = 0

    def stop(self):
        pass

    @property
    def offsets(self):
        if self._offsets is None:
            if self.mock is None:
                self._offsets = self.client.offsets.copy()
            else:
                partitions = self._get_init_params('partitions') or self.client.offsets.copy()
                self._offsets = {p: self.client.offsets[p] for p in partitions}
        return self._offsets

    def _get_init_params(self, keyword, default=None):
        return self.mock.call_args[1].get(keyword, default)


LatestOffsetsResponse = namedtuple('LatestOffsetsResponse', ['partition', 'offsets'])


class FakeKafkaConsumer(FakeConsumer):
    def __init__(self, client, mock=None, fail_on_offset=None):
        self._client = client
        self.__itermsgs = None
        self.assignment = None
        super(FakeKafkaConsumer, self).__init__(client, mock, fail_on_offset)
        self.config = {
            'group_id': self._get_init_params('group_id'),
        }

    def assign(self, topic_partitions):
        self.assignment = list(topic_partitions)
        self._offsets = {}
        if self.config['group_id'] is None:
            for p in topic_partitions:
                self._offsets[p.partition] = self._client.latest_offsets[p.partition]
        else:
            self._offsets = {p.partition: self.client.offsets[p.partition] for p in topic_partitions}

    def position(self, topic_partition):
        return self.offsets[topic_partition.partition]

    def topics(self):
        return self._client.topics

    def close(self):
        pass

    def seek(self, partition, offset):
        self.offsets[partition.partition] = offset

    def __iter__(self):
        if self.__itermsgs is None:
            def _itermsgs():
                while True:
                    result = self.get_records()
                    if result:
                        yield result[0]
                    else:
                        raise StopIteration

            self.__itermsgs = _itermsgs()
        return self

    def next(self):
        return next(self.__itermsgs)

    @property
    def offsets(self):
        if self._offsets is None:
            if self.mock is None:
                self._offsets = self.client.offsets.copy()
        return self._offsets


class FakeClient(object):
    def __init__(self, data, num_partitions=1, max_partition_messages=None, count_variations=None):
        self.topic_partitions = {'test-topic': {i: None for i in range(num_partitions)}}
        self.data = {}
        self.latest_offsets = {p: 0 for p in range(num_partitions)}
        self.offsets = {p: 0 for p in range(num_partitions)}
        # this one simulates variability in records per partition retrieved by the server
        self.count_variations = count_variations or {p: 0 for p in range(num_partitions)}

        partitions = cycle(range(num_partitions))
        if max_partition_messages:
            assert sum(max_partition_messages.values()) >= len(data)
            for msg in data:
                while True:
                    p = partitions.next()
                    if self.latest_offsets[p] < max_partition_messages[p]:
                        self.data.setdefault(p, []).append(msg)
                        self.latest_offsets[p] += 1
                        break
        else:
            for msg in data:
                partition = int(msg.key[2:]) % num_partitions
                self.data.setdefault(partition, []).append(msg)
                self.latest_offsets[partition] += 1

    def get_msg_generator(self, count, offsets):
        for partition, init_offset in offsets.items():
            for offset in range(init_offset, max(min(self.latest_offsets[partition],
                        init_offset + count + self.count_variations[partition]), 0)):
                msg = self.data[partition][offset]
                yield ConsumerRecord(partition, offset, msg.key, msg.value)

    def send_offset_request(self, *args, **kwargs):
        result = []
        for p, o in self.latest_offsets.items():
            result.append(LatestOffsetsResponse(p, [o]))
        return result

    @property
    def topics(self):
        return self.topic_partitions.keys()

    def close(self):
        pass

def create_fake_consumer(client_mock, consumer_mock, fail_on_offset=None):
    def _side_effect(*args, **kwargs):
        return FakeConsumer(client_mock.return_value, consumer_mock, fail_on_offset)
    return _side_effect

def create_fake_kafka_consumer(client_mock, consumer_mock, fail_on_offset=None):
    def _side_effect(*args, **kwargs):
        return FakeKafkaConsumer(client_mock.return_value, consumer_mock, fail_on_offset)
    return _side_effect
