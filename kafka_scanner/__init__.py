import os
import time
import shutil
import tempfile
import atexit
import threading
import logging

from collections import Iterable, defaultdict, OrderedDict

from retrying import retry
import kafka
from sqlitedict import SqliteDict

from .msg_processor import MsgProcessor
from .multiprocess import ExtendedMultiProcessConsumer
from .utils import retry_on_exception


DEFAULT_BATCH_SIZE = 10000
FETCH_BUFFER_SIZE_BYTES = 10 * 1024 * 1024
FETCH_SIZE_BYTES = 10 ** 7
MAX_FETCH_BUFFER_SIZE_BYTES = FETCH_BUFFER_SIZE_BYTES * 10
_MIN_SEEK_SAMPLE_SIZE = 50

__all__ = ['KafkaScanner', 'KafkaScannerDirect', 'KafkaScannerSimple']
logging.getLogger("kafka.client").setLevel(logging.WARNING)


log = logging.getLogger(__name__)


import sys, socket
if sys.version_info < (2, 7, 4):
    # workaround for: http://bugs.python.org/issue6056
    socket.setdefaulttimeout(None)


class keydefaultdict(defaultdict):
    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError( key )
        else:
            ret = self[key] = self.default_factory(key)
            return ret


def _startswith(mystr, prefixes=None, start_after=None):
    """
    Returns True if mystr starts with any of the given prefixes or is alphabetically above
    start_after prefix. False otherwise.
    """
    if not prefixes and not start_after:
        return True
    prefixes = prefixes or []
    for prefix in prefixes:
        if mystr.startswith(prefix):
            return True
    if start_after and mystr > start_after:
        return True
    return False


class MessageCache(object):
    def __init__(self, unique_keys=True):
        self._unique_keys = unique_keys
        if unique_keys:
            self._cache = OrderedDict()
        else:
            self._cache = list()
            self._keys = list()

    def append(self, record):
        if self._unique_keys:
            self._cache[record['_key']] = record
        else:
            self._cache.append(record)
            self._keys.append(record['_key'])

    def values(self):
        if self._unique_keys:
            return self._cache.values()
        return list(self._cache)

    def __contains__(self, key):
        if self._unique_keys:
            return key in self._cache
        return key in self._keys

    def __getitem__(self, key):
        if self._unique_keys:
            return self._cache[key]
        return self._cache[self._keys.index(key)]

    def get(self, key, default=None):
        if key in self:
            return self[key]
        return default

    def __len__(self):
        return len(self._cache)


# kafka-python 0.9.4 has not absolute offset seek
# feature is added on current development branch
def _seek_consumer(consumer, absolute_offset):
    for partition in consumer.offsets:
        consumer.offsets[partition] = absolute_offset
    # apply original method reset/fetch
    consumer.seek(0, 1)

@retry(wait_fixed=60000, retry_on_exception=retry_on_exception)
def get_latest_offsets(consumer, topic, partitions=None):
    partitions = partitions or consumer.offsets.keys()
    result = {}
    reqs = []

    for partition in partitions:
        reqs.append(kafka.common.OffsetRequest(topic, partition, -1, 1))

    resps = consumer.client.send_offset_request(reqs)
    for resp in resps:
        result[resp.partition] = resp.offsets[0]

    return result


class StatsLogger(object):
    def __init__(self):
        self._stats_logline = ''
        self._stats_logline_totals = ''
        self._stats_getters = []
        self.closed = False
        self.periodic_log_thread = threading.Thread(target=self.periodic_log)
        self.periodic_log_thread.daemon = True
        self.periodic_log_thread.start()

    def append_stat_var(self, name, get_func):
        """
        get_func is a function that returns a tuple (name, value)
        """
        self._stats_getters.append(get_func)
        self._stats_logline += '%s: {}. ' % name
        self._stats_logline_totals += 'Total %s: {}. ' % name

    def log_stats(self, prefix='', totals=False):
        stats = [g() for g in self._stats_getters]
        logline = self._stats_logline_totals if totals else self._stats_logline
        log.info(prefix + logline.format(*stats))

    def periodic_log(self):
        while not self.closed:
            time.sleep(600)
            self.log_stats('Activity: ')

    def close(self):
        self.closed = True


class KafkaScanner(object):

    def __init__(self, brokers, topic, group, batchsize=DEFAULT_BATCH_SIZE, count=0,
                        batchcount=0, keep_offsets=False, nodelete=False, nodedupe=False,
                        partitions=None, max_next_messages=10000, logcount=10000,
                        start_offsets=None, min_lower_offsets=None, key_prefixes=None,
                        start_after=None, encoding='utf8'):
        """ Scanner class using Kafka as a source for the dumper
        supported kwargs:

        keep_offsets - don't initialize partition read offsets. Instead use last registered (enable for resuming jobs)
        nodedupe - yield all records, regardless duplicated keys. Default is to dedupe.
        nodelete - yield also deletion records
        count - number of records to yield
        batchsize - number of records per batch. Bigger is more efficient, but more memory demanding.
                    Defaults to DEFAULT_BATCH_SIZE
        batchcount - max number of batches to yield (no limited if 0)
        partitions - set which partitions to scan
        logcount - scanned records period to print stats log line
        start_offsets - Set starting upper offsets dict. If None, upper offsets will be set to latest offsets for each
                        partition (except if keep_offsets is True)
        min_lower_offsets - Set limit lower offsets until which to scan.
        key_prefixes - Only yield records with given key prefixes. Has predecende over start_after.
        start_after - Only yield records with key prefixes after the given one.
        encoding - encoding to pass to msgpack.unpackb in order to return unicode strings
        """
        assert isinstance(brokers, Iterable)
        if keep_offsets:
            assert group, 'keep_offsets option needs a group name'
        self._client = kafka.KafkaClient(map(bytes, brokers))
        self._topic = bytes(topic)
        self._group = bytes(group) if isinstance(group, basestring) else None
        self._partitions = partitions
        self.enabled = False
        self.__real_scanned_count = 0
        self.__scanned_count = 0
        self.__deleted_count = 0
        self.__issued_count = 0
        self.__dupes_count = 0
        self.__encoding = encoding
        self.__batchcount = batchcount
        self.__issued_batches = 0

        self.__logcount = logcount
        self.consumer = None
        self.init_consumer = None
        self.processor = None
        self.processor_handlers = None
        self._min_lower_offsets = defaultdict(int, min_lower_offsets or {})
        self._lower_offsets = None
        self._upper_offsets = None
        self._latest_offsets = start_offsets
        self._key_prefixes = key_prefixes
        self._start_after = start_after

        self._dupestempdir = None
        self._dupes = None
        if not nodedupe:
            self._dupestempdir = tempfile.mkdtemp()
            self._dupes = keydefaultdict(self._make_dupe_dict)

        self.__batchsize = batchsize or DEFAULT_BATCH_SIZE
        self._count = count
        self._keep_offsets = keep_offsets
        self._nodelete = nodelete

        self.stats_logger = StatsLogger()
        for name, getter in (
                ('Scanned', lambda : self.scanned_count),
                ('Issued', lambda : self.issued_count),
                ('Deleted', lambda : self.deleted_count)):
            self.stats_logger.append_stat_var(name, getter)
        if not nodedupe:
            self.stats_logger.append_stat_var('Dupes', lambda : self.dupes_count)

        self.__max_next_messages = min(max_next_messages, self.__batchsize)
        # ensures cleaning of threads/db even after exceptions
        self.__closed = False
        atexit.register(self.close)

    def _make_dupe_dict(self, partition):
        return SqliteDict(os.path.join(self._dupestempdir, "%s.db" % partition), flag='n', autocommit = True)

    def init_scanner(self):
        handlers_list = ('consume_messages', 'decompress_messages', 'unpack_messages')
        if not self.enabled:
            self.enabled = True
            self.processor = MsgProcessor(handlers_list, encoding=self.__encoding)
        self._create_init_consumer()

    @retry(wait_fixed=60000, retry_on_exception=retry_on_exception)
    def get_commited_offsets(self):
        consumer = kafka.SimpleConsumer(self._client, self._group, self._topic, partitions=self._partitions)
        return consumer.offsets

    @retry(wait_fixed=60000, retry_on_exception=retry_on_exception)
    def seek_key_prefixes(self, partition, start_upper_offset, sample_ratio=100, max_jump=None):
        """
        This works under the assumption that key prefixes are clustered, so we can accelerate scanning for
        getting particular range of keys without scanning the entire topic. It jumps self.__max_next_messages offsets and
        after each jump takes a sample with size self.__max_next_messages / 1000 records to search for the given
        prefixes.

        prefixes - a list of key prefixes to seek to
        partition - target partition
        upper_offset - starting upper offset
        sample_ratio - Ratio between offset jump step (self.__max_next_messages) and sample size.
        max_jump - max offset jump to get next samples. If not given, will be used max_next_messages.
        
        Seek speed up will be essentially sample_ratio (unless self.__max_next_messages is too small, minimal sample size is 10)
        Consider that the bigger the speed up, more probabilities to loose smaller clusters, or some few records from the bigger
        ones near its limits
        """
        def _sample_hit(sample):
            for offset, key in sample:
                if _startswith(key, self._key_prefixes, self._start_after):
                    return offset, key
            return None

        max_jump = min(max_jump or self.__max_next_messages, self.__max_next_messages)
        sample_size = max(_MIN_SEEK_SAMPLE_SIZE, max_jump / sample_ratio)
        if not self._key_prefixes and not self._start_after:
            return start_upper_offset
        cluster_found = None
        consumer = kafka.SimpleConsumer(
                self._client, self._group + '_seeker',
                self._topic, partitions=[partition],
                fetch_size_bytes=FETCH_SIZE_BYTES,
                buffer_size=FETCH_BUFFER_SIZE_BYTES,
                max_buffer_size=MAX_FETCH_BUFFER_SIZE_BYTES,
                auto_commit=False)
        consumer.provide_partition_info()
        upper_offset = start_upper_offset
        self.processor.set_consumer(consumer)
        scanner_sample_size = self.processor.processor_handlers.next_messages
        self.processor.processor_handlers.set_next_messages(sample_size)
        log.info("Start seeking offset: {%s: %s}" % (partition, upper_offset))

        while cluster_found is None and upper_offset > self._min_lower_offsets[partition]:
            lower_offset = upper_offset - max_jump
            lower_offset = max(lower_offset, self._min_lower_offsets[partition])
            _seek_consumer(consumer, lower_offset)
            sample = self._get_sample(sample_size)
                
            cluster_found = _sample_hit(sample)
            if not cluster_found:
                _seek_consumer(consumer, upper_offset - sample_size)
                sample = self._get_sample(sample_size)
                cluster_found = _sample_hit(sample)
            if not cluster_found:
                upper_offset = lower_offset
        
        if cluster_found is not None:
            offset, key = cluster_found
            log.info("Position found: {%s: %s} (%s)" % (partition, offset, key))
            log.info("Upper offset: {%s: %s}" % (partition, upper_offset))
        consumer.stop()
        self.processor.set_consumer(self.consumer)
        self.processor.processor_handlers.set_next_messages(scanner_sample_size)
        return upper_offset

    def _get_sample(self, sample_size):
        return [(offset, record['_key']) for _, offset, record in self.processor.process(sample_size)]

    @retry(wait_fixed=60000, retry_on_exception=retry_on_exception)
    def _create_init_consumer(self):
        self.init_consumer = kafka.SimpleConsumer(self._client, self._group, self._topic, partitions=self._partitions)

    @retry(wait_fixed=60000, retry_on_exception=retry_on_exception)
    def _commit_offsets(self, offsets):
        log.info('Commiting offsets: {}'.format(offsets))
        self.init_consumer.offsets.update(offsets)
        self.init_consumer.count_since_commit += 1
        self.init_consumer.commit()

    def _init_offsets(self, batchsize):
        upper_offsets = self._lower_offsets
        if not upper_offsets:
            upper_offsets = {p: o for p, o in self.init_consumer.offsets.items()}
            if not self._group or not self._keep_offsets:
                upper_offsets = self.latest_offsets
        self._upper_offsets = {p: o for p, o in upper_offsets.items() if o > self._min_lower_offsets[p]}
        partition_batchsize = 0
        if self._upper_offsets:
            partition_batchsize = batchsize / len(self._upper_offsets) or 1
            for p in self._upper_offsets:
                self._upper_offsets[p] = self.seek_key_prefixes(p, self._upper_offsets[p], max_jump=partition_batchsize)
            self._lower_offsets = {p: max(self._upper_offsets[p] - partition_batchsize, \
                            self._min_lower_offsets[p]) for p in self._upper_offsets}
            # sub consumers must start from newly computed lower offsets
            self._commit_offsets(self._lower_offsets)
        return partition_batchsize

    def _init_scan_consumer(self, batchsize):
        if self.consumer is not None:
            self.consumer.stop()
        previous_lower_offsets = self._lower_offsets

        partition_batchsize = self._init_offsets(batchsize)
        self._create_scan_consumer()

        # commit previous lower offsets in order to read correct latest offsets if this job fails
        if previous_lower_offsets:
            self._commit_offsets(previous_lower_offsets)

        return partition_batchsize

    @retry(wait_fixed=60000, retry_on_exception=retry_on_exception)
    def _create_scan_consumer(self):
        self.consumer = ExtendedMultiProcessConsumer(
            client=self._client,
            partitions=self._upper_offsets.keys(),
            auto_commit=False,
            group=self._group,
            topic=self._topic,
            num_procs=len(self._upper_offsets),
            fetch_size_bytes=FETCH_SIZE_BYTES,
            buffer_size=FETCH_BUFFER_SIZE_BYTES,
            max_buffer_size=MAX_FETCH_BUFFER_SIZE_BYTES,
            iter_timeout=60,
        )
        self.consumer.provide_partition_info()
        self.processor.set_consumer(self.consumer)
        log.info("Initial offsets: {}".format(repr(self.consumer.offsets)))
        log.info("Target offsets: {}".format(repr(self._upper_offsets)))

    def _scan_topic_batch(self, partition_batchsize):

        if not self._lower_offsets: # there is nothing to process
            self.enabled = False
            return

        max_next_messages = min(partition_batchsize, self.__max_next_messages)
        messages = MessageCache(self._dupes is not None)

        while self.enabled and self.are_there_batch_messages_to_process(len(messages)):
            for partition, offset, record in self.processor.process(max_next_messages):
                lastkey = record['_key']
                self.__real_scanned_count += 1
                if offset < self._upper_offsets[partition]:
                    self.__scanned_count += 1
                    if _startswith(lastkey, self._key_prefixes, self._start_after) \
                                and (lastkey in messages or not self._record_is_dupe(partition, lastkey)):
                        if self.must_delete_record(record):
                            self.__deleted_count += 1
                            if messages.get(lastkey, None) is not None and self.__issued_count > 0:
                                self.__issued_count -= 1
                        elif lastkey not in messages or self.must_delete_record(messages[lastkey]):
                            self.__issued_count += 1
                        else:
                            self.__dupes_count += 1
                        messages.append(record)

                if self.__real_scanned_count % self.__logcount == 0:
                    self.stats_logger.log_stats('Last key: {} '.format(lastkey))

                # keep control of scanned count if defined
                if self.__issued_count > 0 and self.__issued_count == self._count:
                    self.enabled = False
                    break

        return messages.values()

    def _record_is_dupe(self, partition, key):
        if self._dupes is None:
            return False
        if key not in self._dupes[partition]:
            self._dupes[partition][key] = ''
            return False
        self.__dupes_count += 1
        return True

    def _filter_deleted_records(self, records):
        """
        Filter out deleted records
        """
        while records:
            record = records.pop(0)
            if not self.must_delete_record(record):
                yield record

    def _process_records(self, records):
        for record in records:
            yield self.process_record(record)

    def get_new_batch(self):

        batchsize = self.__batchsize
        if self._count > 0:
            batchsize = min(self.__batchsize, self._count - self.__issued_count)

        pipeline = batchsize
        for processor in [self._init_scan_consumer,
                          self._scan_topic_batch,
                          self._filter_deleted_records,
                          self._process_records]:
            pipeline = processor(pipeline)

        log.info("Last offsets: {}".format(repr(self.consumer.offsets)))
        return pipeline

    def must_delete_record(self, record):
        return len(record) == 1 and not self._nodelete

    def scan_topic_batches(self):
        self.init_scanner()
        messages = []
        while self.enabled:
            if self.consumer is None or self.are_there_messages_to_process():
                for message in self.get_new_batch():
                    if self.__batchcount > 0 and self.__issued_batches == self.__batchcount - 1:
                        self.enabled = False
                    if len(messages) == self.__batchsize:
                        yield messages
                        messages = []
                        self.__issued_batches += 1
                    messages.append(message)
            else:
                break
        if messages:
            yield messages
            self.__issued_batches += 1

        self.commit_final_offsets()

        self.stats_logger.log_stats(totals=True)

        log.info("Total batches Issued: %d", self.__issued_batches)
        scan_efficiency = 100.0 - ( 100.0 * (self.__real_scanned_count - \
                        self.scanned_count) / self.__real_scanned_count) \
                        if self.__real_scanned_count else 100.0
        log.info("Real Scanned: {}".format(self.__real_scanned_count))
        log.info("Scan efficiency: {:.2f}%".format(scan_efficiency))

        self.close()

    def close(self):
        if not self.__closed:
            self.__closed = True
            self.stats_logger.close()
            if self.consumer is not None:
                self.consumer.stop()
            if self.init_consumer is not None:
                self.init_consumer.stop()
                self.init_consumer.client.close()
            if self._dupes is not None:
                for db in self._dupes.values():
                    db.close()
                shutil.rmtree(self._dupestempdir)

    def commit_final_offsets(self):
        # may appear holes if number of partitions is not divisor of count
        diff_offsets = {p: max(self._upper_offsets.get(p, 0) - o, 0) for p, o in self.consumer.offsets.items()}
        if any(diff_offsets.values()):
            commit_offsets = {p: self._lower_offsets.get(p, 0) + o for p, o in diff_offsets.items()}
            self._commit_offsets(commit_offsets)

    def run(self):
        """ Convenient method for iterating along topic. """

        for batch in self.scan_topic_batches():
            for _ in batch:
                pass

        return self.issued_count

    def process_record(self, record):
        return record

    def are_there_messages_to_process(self):
        for partition, offset in self._lower_offsets.items():
            if offset > self._min_lower_offsets[partition]:
                return True
        return False

    def are_there_batch_messages_to_process(self, msgslen):
        for partition, offset in self._upper_offsets.items():
            if self.consumer.offsets[partition] < offset:
                return True
        return False

    @property
    def scanned_count(self):
        return self.__scanned_count

    @property
    def deleted_count(self):
        return self.__deleted_count

    @property
    def issued_count(self):
        return self.__issued_count

    @property
    def batchsize(self):
        return self.__batchsize

    @property
    def dupes_count(self):
        return self.__dupes_count

    @property
    def latest_offsets(self):
        if not self._latest_offsets:
            self._latest_offsets = get_latest_offsets(self.init_consumer, self._topic, self._partitions)
        return self._latest_offsets


class KafkaScannerSimple(KafkaScanner):
    """
    Scanner implemented around a Kafka SimpleConsumer
    """
    @retry(wait_fixed=60000, retry_on_exception=retry_on_exception)
    def _create_scan_consumer(self):
        self.consumer = kafka.SimpleConsumer(
            client=self._client,
            partitions=self._upper_offsets.keys(),
            auto_commit=False,
            group=self._group,
            topic=self._topic,
            fetch_size_bytes=FETCH_SIZE_BYTES,
            buffer_size=FETCH_BUFFER_SIZE_BYTES,
            max_buffer_size=MAX_FETCH_BUFFER_SIZE_BYTES,
            iter_timeout=60,
        )
        self.consumer.provide_partition_info()
        self.processor.set_consumer(self.consumer)
        log.info("Initial offsets: {}".format(repr(self.consumer.offsets)))
        log.info("Target offsets: {}".format(repr(self._upper_offsets)))


class KafkaScannerDirect(KafkaScannerSimple):
    """
    Scanner in direct sense. Dedupe is not supported in order to conserve
    logic of direct scanning. Also, delete records are issued.

    This is essentially a wrapper around SimpleConsumer for supporting same api than other scanners,
    with few extra feature support)

    start_offsets - allow to set start offsets dict.

    The rest of parameters has the same functionality as parent class
    """
    def __init__(self, brokers, topic, group, batchsize=DEFAULT_BATCH_SIZE, batchcount=0, keep_offsets=False,
            partitions=None, start_offsets=None, max_next_messages=10000, logcount=10000):
        super(KafkaScannerDirect, self).__init__(brokers, topic, group, batchsize=batchsize,
                    count=0, batchcount=batchcount, keep_offsets=keep_offsets, nodelete=True, nodedupe=True,
                    partitions=partitions, max_next_messages=max_next_messages, logcount=logcount)
        self._lower_offsets = start_offsets

    def init_scanner(self):
        super(KafkaScannerDirect, self).init_scanner()
        if not self._group or not self._keep_offsets or self._lower_offsets is not None:
            if self._lower_offsets is None:
                self._lower_offsets = {partition: 0 for partition in self.init_consumer.offsets}
            self.init_consumer.offsets.update(self._lower_offsets)
            self.init_consumer.count_since_commit += 1
            self.init_consumer.commit()
        else:
            self._lower_offsets = self.init_consumer.offsets.copy()
        self._upper_offsets = self.latest_offsets
        self._create_scan_consumer()

    def _init_offsets(self, batchsize):
        self._lower_offsets = self.consumer.offsets.copy() 
        return batchsize / len(self._upper_offsets) or 1

    def _init_scan_consumer(self, batchsize):
        previous_lower_offsets = self._lower_offsets

        partition_batchsize = self._init_offsets(batchsize)

        # commit previous lower offsets in order to read correct latest offsets if this job fails
        if previous_lower_offsets:
            self._commit_offsets(previous_lower_offsets)
        return partition_batchsize

    def commit_final_offsets(self):
        commit_offsets = self.consumer.offsets
        self._commit_offsets(commit_offsets)

    def are_there_messages_to_process(self):
        for partition, offset in self.consumer.offsets.items():
            if offset < self.latest_offsets[partition]:
                return True
        return False

    def are_there_batch_messages_to_process(self, msgslen):
        if msgslen > self.batchsize:
            return False
        return super(KafkaScannerDirect, self).are_there_batch_messages_to_process(msgslen)
