# coding=utf-8
"""
Tools for mocking kafka-python objects in order to allow to build tests
"""

import zlib
from collections import namedtuple
from itertools import cycle

import msgpack


OffsetAndMessage = namedtuple("OffsetAndMessage", ["offset", "message"])
Message = namedtuple("Message", ["magic", "attributes", "key", "value"])


def get_kafka_msg_samples(msgs=None, fetch_count=0):
    if not msgs:
        msgs = [('AD12345', "my-body"),
                ('AD34567', "second my-body"),
                ('AD67890', 'third my-body')]
    fetch_count = fetch_count or len(msgs)
    return [Message(0, 0, key,
         zlib.compress(msgpack.packb({'body': body})) if body else None)
         for key, body in msgs[:fetch_count]]


class FakeConsumer(object):
    def __init__(self, client, mock=None, fail_on_offset=None):
        self.client = client
        self.count_since_commit = 0
        self._offsets = None
        self.msg_queue = []
        self.mock = mock
        self.partition_info = False
        self.fail_on_offset = fail_on_offset

    def provide_partition_info(self):
        self.partition_info = True

    def get_messages(self, count=1, *args, **kwargs):
        purged_partitions = []
        while self.msg_queue:
            p, omsg = self.msg_queue[0]
            if p in purged_partitions:
                break
            if omsg.offset != self.offsets[p]:
                self.msg_queue.pop(0)
            else:
                purged_partitions.append(p)
        messages = []
        while count:
            if not self.msg_queue:
                for pm in self.client.get_msg_generator(count, self.offsets):
                    self.msg_queue.append(pm)
            if self.msg_queue:
                partition, omsg = self.msg_queue.pop(0)
                assert (self.fail_on_offset != omsg.offset), 'Failed on offset {}'.format(omsg.offset)
                self.offsets[partition] = omsg.offset + 1
                _msg = (partition, omsg) if self.partition_info else omsg
                messages.append(_msg)
                count -= 1
            else:
                break
        self.count_since_commit += len(messages)
        self._auto_commit()
        return messages

    def _auto_commit(self):
        # exactly as in kafka.consumer.base.Consumer
        # Check if we are supposed to do an auto-commit
        if not self.auto_commit or self.auto_commit_every_n is None:
            return
        if self.count_since_commit >= self.auto_commit_every_n:
            self.commit()

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

    @property
    def auto_commit(self):
        return self._get_init_params('auto_commit', True)

    @property
    def auto_commit_every_n(self):
        return self._get_init_params('auto_commit_every_n', 100)

LatestOffsetsResponse = namedtuple('LatestOffsetsResponse', ['partition', 'offsets'])


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
                yield partition, OffsetAndMessage(offset, self.data[partition][offset])

    def send_offset_request(self, *args, **kwargs):
        result = []
        for p, o in self.latest_offsets.items():
            result.append(LatestOffsetsResponse(p, [o]))
        return result

    def close(self):
        pass

def create_fake_consumer(client_mock, consumer_mock, fail_on_offset=None):
    def _side_effect(*args, **kwargs):
        return FakeConsumer(client_mock.return_value, consumer_mock, fail_on_offset)
    return _side_effect
