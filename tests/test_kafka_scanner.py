# coding=utf-8
import unittest

from mock import patch

from kafka_scanner import KafkaScanner, KafkaScannerDirect
from kafka_scanner.tests import (
        get_kafka_msg_samples,
        FakeClient,
        create_fake_consumer,
        create_fake_kafka_consumer)

class BaseScannerTest(unittest.TestCase):
    scannerclass = KafkaScanner
    def _get_scanner_messages(self, client_mock, kafka_consumer_mock, simple_consumer_mock,
                fail_on_offset=None, **scanner_kwargs):
        simple_consumer_mock.side_effect = create_fake_consumer(client_mock, simple_consumer_mock, fail_on_offset)
        kafka_consumer_mock.side_effect = create_fake_kafka_consumer(client_mock, kafka_consumer_mock, None)
        topic = 'test-topic'
        group = scanner_kwargs.pop('group', None)
        scanner = self.scannerclass(['kafka:9092'], topic, group,
                partitions=client_mock.return_value.topic_partitions[topic].keys(),
                  **scanner_kwargs)
        batches = scanner.scan_topic_batches()
        number_of_batches = 0
        messages = []
        try:
            for batch in batches:
                number_of_batches += 1
                for m in batch:
                    messages.append(m)
        except AssertionError:
            pass
        return scanner, number_of_batches, messages


@patch('kafka.SimpleConsumer', autospec=True)
@patch('kafka.KafkaConsumer', autospec=True)
@patch('kafka.KafkaClient', autospec=True)
class KafkaScannerTest(BaseScannerTest):

    msgs = [('AD%d' % i, 'body %d' % i) for i in range(7)]
    samples = get_kafka_msg_samples(msgs)

    def test_kafka_scan(self, client_mock, kafka_consumer_mock, simple_consumer_mock, num_partitions=1, expected_batches=1):
        expected_messages = 7
        client_mock.return_value = FakeClient(self.samples, num_partitions)
        _, number_of_batches, messages = self._get_scanner_messages(client_mock, kafka_consumer_mock, simple_consumer_mock,
                nodedupe=True)
        msgkeys = [m['_key'] for m in messages]
        self.assertEqual(len(set(msgkeys)), expected_messages)
        self.assertEqual(len(msgkeys), expected_messages)
        self.assertEqual(number_of_batches, expected_batches)

    def test_kafka_scan_partitions(self, client_mock, kafka_consumer_mock, simple_consumer_mock):
        self.test_kafka_scan(num_partitions=3, expected_batches=1)

    def test_kafka_scan_count(self, client_mock, kafka_consumer_mock, simple_consumer_mock, num_partitions=1, expected_batches=1):
        expected_messages = 2
        client_mock.return_value = FakeClient(self.samples, num_partitions)
        _, number_of_batches, messages = self._get_scanner_messages(client_mock, kafka_consumer_mock, simple_consumer_mock,
                nodedupe=True, count=2)
        msgkeys = [m['_key'] for m in messages]
        self.assertEqual(len(set(msgkeys)), expected_messages)
        self.assertEqual(len(msgkeys), expected_messages)
        self.assertEqual(number_of_batches, expected_batches)

    def test_kafka_scan_count_partitions(self, client_mock, kafka_consumer_mock, simple_consumer_mock):
        self.test_kafka_scan_count(num_partitions=3)

    def test_kafka_scan_batchsize(self, client_mock, kafka_consumer_mock, simple_consumer_mock, num_partitions=1):
        expected_messages = 7
        expected_batches = 4
        client_mock.return_value = FakeClient(self.samples, num_partitions)
        _, number_of_batches, messages = self._get_scanner_messages(client_mock, kafka_consumer_mock, simple_consumer_mock,
                nodedupe=True, batchsize=2)
        msgkeys = [m['_key'] for m in messages]
        self.assertEqual(len(set(msgkeys)), expected_messages)
        self.assertEqual(len(msgkeys), expected_messages)
        self.assertEqual(number_of_batches, expected_batches)

    def test_kafka_scan_batchsize_partitions(self, client_mock, kafka_consumer_mock, simple_consumer_mock):
        self.test_kafka_scan_batchsize(num_partitions=3)

    def test_kafka_scan_batchsize_count(self, client_mock, kafka_consumer_mock, simple_consumer_mock, num_partitions=1):
        expected_messages = 5
        expected_batches = 3
        client_mock.return_value = FakeClient(self.samples, num_partitions)
        _, number_of_batches, messages = self._get_scanner_messages(client_mock, kafka_consumer_mock, simple_consumer_mock,
                nodedupe=True, batchsize=2, count=5)
        msgkeys = [m['_key'] for m in messages]
        self.assertEqual(len(set(msgkeys)), expected_messages)
        self.assertEqual(len(msgkeys), expected_messages)
        self.assertEqual(number_of_batches, expected_batches)

    def test_kafka_scan_batchsize_count_partitions(self, client_mock, kafka_consumer_mock, simple_consumer_mock):
        self.test_kafka_scan_batchsize_count(num_partitions=3)

    def test_kafka_scan_batchcount(self, client_mock, kafka_consumer_mock, simple_consumer_mock,
                batchsize=10000, batchcount=3, num_partitions=1, expected_messages=1000):
         msgs = [('AD%.3d' % i, 'body %d' % i) for i in range(1000)]
         samples = get_kafka_msg_samples(msgs)
         client_mock.return_value = FakeClient(samples, num_partitions, count_variations={0: 2, 1: 3, 2: 2})
         scanner, number_of_batches, messages = self._get_scanner_messages(client_mock, kafka_consumer_mock, simple_consumer_mock,
                batchsize=batchsize, batchcount=batchcount)
         self.assertEqual(number_of_batches, min(batchcount, 1000 / batchsize or 1))
         msgkeys = [m['_key'] for m in messages]
         self.assertEqual(len(msgkeys), expected_messages)
         self.assertEqual(len(set(msgkeys)), expected_messages)

    def test_kafka_scan_batchcount_batches(self, client_mock, kafka_consumer_mock, simple_consumer_mock):
        self.test_kafka_scan_batchcount(batchsize=200, expected_messages=600)

    def test_kafka_scan_batchcount_one_batch(self, client_mock, kafka_consumer_mock, simple_consumer_mock):
        self.test_kafka_scan_batchcount(batchsize=200, batchcount=1, expected_messages=200)

    def test_kafka_scan_batchcount_partitions(self, client_mock, kafka_consumer_mock, simple_consumer_mock):
        self.test_kafka_scan_batchcount(num_partitions=3)

    def test_kafka_scan_batchcount_batches_partitions(self, client_mock, kafka_consumer_mock, simple_consumer_mock):
        self.test_kafka_scan_batchcount(batchsize=200, num_partitions=3, expected_messages=600)

    def test_kafka_scan_dedupe(self, client_mock, kafka_consumer_mock, simple_consumer_mock, batchsize=10000):
        msgs = [('AD%.3d' % i, 'body %d' % i) for i in range(1000)] + \
                [('AD%.3d' % i, 'body %dA' % i) for i in range(100, 200)]
        samples = get_kafka_msg_samples(msgs)

        client_mock.return_value = FakeClient(samples, 3, count_variations={0: 2, 1: 3, 2: 2})
        scanner, _, messages = self._get_scanner_messages(client_mock, kafka_consumer_mock, simple_consumer_mock,
                batchsize=batchsize)
        msgsdict = {m['_key']: m['body'] for m in messages}

        self.assertEqual(len(msgsdict), 1000)
        self.assertEqual(scanner.issued_count, 1000)
        self.assertEqual(scanner.scanned_count, 1100)
        self.assertEqual(scanner.dupes_count, 100)
        for i in range(100, 200):
            self.assertEqual(msgsdict['AD%.3d' %i], 'body %dA' % i)

    def test_kafka_scan_dedupe_batches(self, client_mock, kafka_consumer_mock, simple_consumer_mock):
        self.test_kafka_scan_dedupe(batchsize=200)

    def test_kafka_scan_deleted(self, client_mock, kafka_consumer_mock, simple_consumer_mock, batchsize=10000):
        msgs = [('AD%.3d' % i, 'body %d' % i) for i in range(1000)] + \
                [('AD%.3d' % i, None) for i in range(100, 200)]
        samples = get_kafka_msg_samples(msgs)

        client_mock.return_value = FakeClient(samples, 3, count_variations={0: 2, 1: 3, 2: 2})
        scanner, _, messages = self._get_scanner_messages(client_mock, kafka_consumer_mock, simple_consumer_mock, batchsize=batchsize)
        msgsdict = {m['_key']: m['body'] for m in messages}
        self.assertEqual(len(set(msgsdict)), 900)
        self.assertEqual(scanner.scanned_count, 1100)
        self.assertEqual(scanner.issued_count, 900)
        self.assertEqual(scanner.dupes_count, 100)
        self.assertEqual(scanner.deleted_count, 100)
        for i in range(100, 200):
            self.assertTrue('AD%.3d' % i not in msgsdict)

    def test_kafka_scan_deleted_batches(self, client_mock, kafka_consumer_mock, simple_consumer_mock):
        self.test_kafka_scan_deleted(batchsize=200)

    def test_kafka_scan_deleted_before(self, client_mock, kafka_consumer_mock, simple_consumer_mock, batchsize=10000):
        msgs = [('AD%.3d' % i, None) for i in range(100, 200)] + \
                [('AD%.3d' % i, 'body %d' % i) for i in range(1000)]

        samples = get_kafka_msg_samples(msgs)

        client_mock.return_value = FakeClient(samples, 3, count_variations={0: 2, 1: 3, 2: 2})
        scanner, _, messages = self._get_scanner_messages(client_mock, kafka_consumer_mock, simple_consumer_mock, batchsize=batchsize)
        msgsdict = {m['_key']: m['body'] for m in messages}

        self.assertEqual(len(set(msgsdict)), 1000)
        self.assertEqual(scanner.scanned_count, 1100)
        self.assertEqual(scanner.issued_count, 1000)
        self.assertEqual(scanner.dupes_count, 100)
        self.assertEqual(scanner.deleted_count, 0)

    def test_kafka_scan_deleted_before_batches(self, client_mock, kafka_consumer_mock, simple_consumer_mock):
        self.test_kafka_scan_deleted_before(batchsize=200)

    def test_kafka_scan_nodelete(self, client_mock, kafka_consumer_mock, simple_consumer_mock):
        msgs = [('AD%.3d' % i, 'body %d' % i) for i in range(1000)] + \
                [('AD%.3d' % i, None) for i in range(100, 200)]
        samples = get_kafka_msg_samples(msgs)

        client_mock.return_value = FakeClient(samples, 3, count_variations={0: 2, 1: 3, 2: 2})
        client_mock.return_value = FakeClient(samples, 3, count_variations={0: 2, 1: 3, 2: 2})
        scanner, _, messages = self._get_scanner_messages(client_mock, kafka_consumer_mock, simple_consumer_mock,
                nodelete=True)
        msgsdict = {m['_key']: m.get('body', None) for m in messages}

        self.assertEqual(len(set(msgsdict)), 1000)
        self.assertEqual(scanner.scanned_count, 1100)
        self.assertEqual(scanner.issued_count, 1000)
        self.assertEqual(scanner.dupes_count, 100)
        self.assertEqual(scanner.deleted_count, 0)
        for i in range(100, 200):
            self.assertEqual(msgsdict['AD%.3d' % i], None)

    def test_kafka_scan_dedupe_many(self, client_mock, kafka_consumer_mock, simple_consumer_mock):
        msgs = [('AD%.3d' % i, 'body %d' % i) for i in range(1000)] * 2
        samples = get_kafka_msg_samples(msgs)

        client_mock.return_value = FakeClient(samples, 3, count_variations={0: 2, 1: 3, 2: 2})
        scanner, _, messages = self._get_scanner_messages(client_mock, kafka_consumer_mock, simple_consumer_mock,
                batchsize=250, logcount=250)
        msgsdict = {m['_key']: m['body'] for m in messages}

        self.assertEqual(len(msgsdict), 1000)
        self.assertEqual(scanner.issued_count, 1000)
        self.assertEqual(scanner.scanned_count, 2000)
        self.assertEqual(scanner.dupes_count, 1000)

    def test_kafka_scan_lower_offsets(self, client_mock, kafka_consumer_mock, simple_consumer_mock, batchsize=10000):
        msgs = [('AD%.3d' % i, 'body %d' % i) for i in range(1000)] + \
                [('AD%.3d' % i, None) for i in range(100, 200)]
        samples = get_kafka_msg_samples(msgs)

        client_mock.return_value = FakeClient(samples, 3, count_variations={0: 2, 1: 3, 2: 2})
        scanner, _, messages = self._get_scanner_messages(client_mock, kafka_consumer_mock, simple_consumer_mock,
                batchsize=batchsize, min_lower_offsets={0: 100, 1: 100, 2: 100})
        msgsdict = {m['_key']: m['body'] for m in messages}
        self.assertEqual(len(set(msgsdict)), 700)
        self.assertEqual(scanner.scanned_count, 800)
        self.assertEqual(scanner.issued_count, 700)
        self.assertEqual(scanner.dupes_count, 0)
        self.assertEqual(scanner.deleted_count, 100)
        for i in range(100, 200):
            self.assertTrue('AD%.3d' % i not in msgsdict)

    def test_kafka_scan_lower_offsets_batches(self, client_mock, kafka_consumer_mock, simple_consumer_mock):
        self.test_kafka_scan_lower_offsets(batchsize=200)

    def test_encoding(self, client_mock, kafka_consumer_mock, simple_consumer_mock):
        msgs = [('AD001', u'hol\xc3\xa1'.encode('latin1'))]
        samples = get_kafka_msg_samples(msgs)
        client_mock.return_value = FakeClient(samples, 1)
        _, _, messages = self._get_scanner_messages(client_mock, kafka_consumer_mock, simple_consumer_mock,
            encoding='latin1')
        self.assertEqual(messages[0]['body'], u'hol\xc3\xa1')

    def test_wrong_encoding(self, client_mock, kafka_consumer_mock, simple_consumer_mock):
        msgs = [('AD001', '>\xc4\xee')]
        samples = get_kafka_msg_samples(msgs)
        client_mock.return_value = FakeClient(samples, 1)
        _, _, messages = self._get_scanner_messages(client_mock, kafka_consumer_mock, simple_consumer_mock)
        self.assertEqual(messages, [])


@patch('kafka.SimpleConsumer', autospec=True)
@patch('kafka.KafkaConsumer', autospec=True)
@patch('kafka.KafkaClient', autospec=True)
class KafkaScannerOverrideTest(BaseScannerTest):
    class MyScanner(KafkaScanner):
        test_count = 0
        def process_record(self, record):
            self.test_count += 1
            return record
    scannerclass = MyScanner

    def test_process_record(self, client_mock, kafka_consumer_mock, simple_consumer_mock):
        msgs = [('AD%.3d' % i, 'body %d' % i) for i in range(1000)] + \
                [('AD%.3d' % i, None) for i in range(100, 200)]
        samples = get_kafka_msg_samples(msgs)

        client_mock.return_value = FakeClient(samples, 3, count_variations={0: 2, 1: 3, 2: 2})
        scanner, _, messages = self._get_scanner_messages(client_mock, kafka_consumer_mock, simple_consumer_mock)
        msgsdict = {m['_key']: m['body'] for m in messages}
        self.assertEqual(len(set(msgsdict)), 900)
        self.assertEqual(scanner.scanned_count, 1100)
        self.assertEqual(scanner.issued_count, 900)
        self.assertEqual(scanner.deleted_count, 100)
        self.assertEqual(scanner.dupes_count, 100)
        self.assertEqual(scanner.test_count, 900)
        for i in range(100, 200):
            self.assertTrue('AD%.3d' % i not in msgsdict)


@patch('kafka.SimpleConsumer', autospec=True)
@patch('kafka.KafkaConsumer', autospec=True)
@patch('kafka.KafkaClient', autospec=True)
class KafkaScannerDirectTest(BaseScannerTest):
    scannerclass = KafkaScannerDirect

    msgs = [('AD%.3d' % i, 'body %d' % i) for i in range(1000)] + \
                [('AD%.3d' % i, None) for i in range(100, 200)]
    samples = get_kafka_msg_samples(msgs)

    def test_kafka_scan_batch(self, client_mock, kafka_consumer_mock, simple_consumer_mock):
        client_mock.return_value = FakeClient(self.samples, 3, count_variations={0: 2, 1: 3, 2: 2})
        _, number_of_batches, messages = self._get_scanner_messages(client_mock, kafka_consumer_mock, simple_consumer_mock,
                batchsize=200, group='test_group')
        msgsdict = {m['_key']: m.get('body', None) for m in messages}
        self.assertEqual(len(messages), 1100)
        self.assertEqual(len(set(msgsdict)), 1000)
        self.assertEqual(number_of_batches, 6)

    def test_kafka_scan_batches_batchcount(self, client_mock, kafka_consumer_mock, simple_consumer_mock, batchsize=100, batchcount=3):
        client_mock.return_value = FakeClient(self.samples, 3, count_variations={0: 2, 1: 3, 2: 2})
        _, number_of_batches, messages = self._get_scanner_messages(client_mock, kafka_consumer_mock, simple_consumer_mock,
                batchsize=batchsize, batchcount=batchcount, group='test_group')
        msgsdict = {m['_key']: m['body'] for m in messages}
        self.assertTrue('AD000' in msgsdict)
        self.assertEqual(number_of_batches, 3)
        msgkeys = set(msgsdict.keys())
        self.assertTrue(batchsize * (batchcount - 1) <= len(msgkeys) <= batchsize * batchcount)

    def test_kafka_starting_offsets(self, client_mock, kafka_consumer_mock, simple_consumer_mock):
        client_mock.return_value = FakeClient(self.samples, 3, count_variations={0: 2, 1: 3, 2: 2})
        _, number_of_batches, messages = self._get_scanner_messages(client_mock, kafka_consumer_mock, simple_consumer_mock,
                batchsize=200, start_offsets={0: 150, 1: 150, 2: 200}, group='test_group')
        self.assertEqual(len(messages), 600)
        self.assertEqual(number_of_batches, 3)


@patch('kafka.SimpleConsumer', autospec=True)
@patch('kafka.KafkaConsumer', autospec=True)
@patch('kafka.KafkaClient', autospec=True)
class KafkaScannerDirectResumeTest(BaseScannerTest):
    scannerclass = KafkaScannerDirect
    msgs = [('AD%.3d' % i, 'body %d' % i) for i in range(1000)]
    samples = get_kafka_msg_samples(msgs)

    def test_kafka_scan_resume_simple_partition(self, client_mock, kafka_consumer_mock, simple_consumer_mock, batchsize=100):

        client_mock.return_value = FakeClient(self.samples, 1)

        all_msgkeys = set()
        sum_msgkeys = 0
        for batchcount in (2, 2, 4, 2):
            _, number_of_batches, messages = self._get_scanner_messages(client_mock, kafka_consumer_mock, simple_consumer_mock,
                        keep_offsets=True, batchsize=batchsize, batchcount=batchcount, group='test_group')
            msgkeys = set([m['_key'] for m in messages])
            sum_msgkeys += len(msgkeys)
            all_msgkeys.update(msgkeys)
            self.assertEqual(len(msgkeys), batchcount * batchsize)
            self.assertTrue(batchsize * (batchcount - 1) <= len(msgkeys) <= batchsize * batchcount)
        self.assertEqual(len(all_msgkeys), sum_msgkeys)
        self.assertEqual(sum_msgkeys, 1000)

    def test_kafka_scan_resume_simple_partition_after_fail(self, client_mock, kafka_consumer_mock, simple_consumer_mock, batchsize=100):

        client_mock.return_value = FakeClient(self.samples, 1)

        all_msgkeys = set()
        sum_msgkeys = 0
        for batchcount, fail_on_offset in ((5, 450), (7, None)):
            scanner, number_of_batches, messages = self._get_scanner_messages(client_mock, kafka_consumer_mock, simple_consumer_mock,
                        keep_offsets=True, batchsize=batchsize, batchcount=batchcount, max_next_messages=100, fail_on_offset=fail_on_offset,
                        group='test_group')
            msgkeys = set([m['_key'] for m in messages])
            sum_msgkeys += len(msgkeys)
            all_msgkeys.update(msgkeys)
        self.assertEqual(len(all_msgkeys), sum_msgkeys)
        self.assertEqual(sum_msgkeys, 1000)


#     def test_kafka_scan_resume(self, client_mock, simple_consumer_mock, batchsize=100):
#
#         client_mock.return_value = FakeClient(self.samples, 3, {0: 235, 1: 443, 2: 322}, {0: 2, 1: 2, 2: 2})
#
#         all_msgkeys = set()
#         sum_msgkeys = 0
#         for batchcount in (2, 2, 4, 2):
#             _, number_of_batches, messages = self._get_scanner_messages(client_mock, simple_consumer_mock,
#                         keep_offsets=True, batchsize=batchsize, batchcount=batchcount)
#             msgkeys = set([m['_key'] for m in messages])
#             sum_msgkeys += len(msgkeys)
#             all_msgkeys.update(msgkeys)
#             self.assertEqual(number_of_batches, batchcount)
#             self.assertTrue(batchsize * (batchcount - 1) <= len(msgkeys) <= batchsize * batchcount)
#         self.assertEqual(len(all_msgkeys), sum_msgkeys)
#         self.assertEqual(sum_msgkeys, 1000)
#         # check we got all consecutive keys
#         intkeys = set(int(k.replace('AD', '')) for k in all_msgkeys)
#         self.assertEqual(intkeys, set(range(sum_msgkeys)))
#
