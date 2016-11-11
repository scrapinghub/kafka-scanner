from __future__ import division

import os
import time
import shutil
import tempfile
import atexit
import logging
import traceback

from collections import Iterable, defaultdict, OrderedDict, namedtuple

from retrying import retry
import kafka
from sqlitedict import SqliteDict

from .msg_processor import MsgProcessor

__version__ = '0.1.1'

DEFAULT_BATCH_SIZE = 10000
FETCH_BUFFER_SIZE_BYTES = 10 * 1024 * 1024
FETCH_SIZE_BYTES = 10 ** 7
MAX_FETCH_BUFFER_SIZE_BYTES = FETCH_BUFFER_SIZE_BYTES * 10

__all__ = ['KafkaScanner', 'KafkaScannerDirect', 'KafkaScannerSimple']
logging.getLogger("kafka.client").setLevel(logging.WARNING)
logging.getLogger("kafka.conn").setLevel(logging.WARNING)


log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


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
        while self._cache:
            yield self._pop()

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

    def _pop(self):
        if self._unique_keys:
            record = self._cache.popitem(False)[1]
        else:
            record = self._cache.pop(0)
        return record


class StatsLogger(object):
    def __init__(self):
        self._stats_logline = ''
        self._stats_logline_totals = ''
        self._stats_getters = []
        self.closed = False

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

    def close(self):
        self.closed = True


ClientBrokers = namedtuple('ClientBrokers', ['client', 'brokers'])

_clientbrokers = None

def _get_client(brokers=None, force_newconn=False):
    global _clientbrokers
    if _clientbrokers is not None:
        brokers = brokers or _clientbrokers.brokers
    assert isinstance(brokers, Iterable)
    if force_newconn or _clientbrokers is None:
        _clientbrokers = ClientBrokers(kafka.KafkaClient(map(bytes, brokers)), brokers)
    return _clientbrokers.client


def retry_on_exception(exception):
    log.error("Retried: {}".format(traceback.format_exc()))
    if not isinstance(exception, KeyboardInterrupt):
        # some kinds of issues solve after reconnection
        # _get_client(force_newconn=True)
        return True
    return False



class KafkaScanner(object):

    def __init__(self, brokers, topic, group=None, batchsize=DEFAULT_BATCH_SIZE, count=0,
                        batchcount=0, keep_offsets=False, nodelete=False, nodedupe=False,
                        partitions=None, max_next_messages=10000, logcount=10000,
                        start_offsets=None, min_lower_offsets=None,
                        encoding='utf8', batch_autocommit=True):
        """ Scanner class using Kafka as a source for the dumper
        supported kwargs:

        keep_offsets - don't initialize partition read offsets. Instead use last registered (enable for resuming jobs)
        nodedupe - If True, yield all records, regardless duplicated keys. Default is False (do dedupe)
        nodelete - If True, yield also deletion records. Default is False.
        count - number of records to yield
        batchsize - number of records per batch. Bigger is more efficient, but more memory demanding.
                    Defaults to DEFAULT_BATCH_SIZE
        batchcount - max number of batches to yield (no limited if 0)
        partitions - set which partitions to scan
        logcount - scanned records period to print stats log line
        max_next_messages - max number of messages to retrieve in a single request from kafka server.
        start_offsets - Set starting upper offsets dict. If None, upper offsets will be set to latest offsets for each
                        partition (except if keep_offsets is True)
        min_lower_offsets - Set limit lower offsets until which to scan.
        encoding - encoding to pass to msgpack.unpackb in order to return unicode strings
        batch_autocommit - If True, commit offsets each time a batch is finished
        """
        # for inverse scanning api version doesn't matter
        self._api_version = None

        self._brokers = brokers
        self._topic = topic
        self._check_topic_exists()

        self._group = None
        self._keep_offsets = False
        self._set_consumer_group(group, keep_offsets)
        self._partitions = [kafka.TopicPartition(self._topic, p) for p in partitions]
        self.enabled = False
        self.__real_scanned_count = 0
        self.__scanned_count = 0
        self.__deleted_count = 0
        self.__issued_count = 0
        self.__dupes_count = 0
        self.__encoding = encoding
        self.__batchcount = batchcount
        self.__issued_batches = 0
        self.__batch_autocommit = batch_autocommit

        self.__logcount = logcount
        self.consumer = None
        self.processor = None
        self.processor_handlers = None
        self._min_lower_offsets = defaultdict(int, min_lower_offsets or {})
        self._lower_offsets = None
        self._upper_offsets = None
        self._latest_offsets = start_offsets
        self.__last_message = None

        self._dupestempdir = None
        self._dupes = None
        if not nodedupe:
            self._dupestempdir = tempfile.mkdtemp()
            self._dupes = keydefaultdict(self._make_dupe_dict)

        self.__max_batchsize = batchsize or DEFAULT_BATCH_SIZE
        self.__batchsize = self.__max_batchsize
        self.__scan_excess = 1
        self._count = count
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

        self.__iter_batches = None

    @retry(wait_fixed=60000, retry_on_exception=retry_on_exception, stop_max_attempt_number=60)
    def _check_topic_exists(self):
        consumer = kafka.KafkaConsumer(bootstrap_servers=self._brokers, group_id=None)
        topics = consumer.topics()
        consumer.close()
        if self.topic not in topics:
            raise ValueError("Topic not found: %s" % topic)

    def _make_dupe_dict(self, partition):
        return SqliteDict(os.path.join(self._dupestempdir, "%s.db" % partition), flag='n', autocommit = True)

    def _set_consumer_group(self, group, keep_offsets):
        if group is not None or keep_offsets:
            log.warning("This class does not allow consumer group. Set consumer group to None and keep_offsets to False.")

    def init_scanner(self):
        handlers_list = ('consume_messages',)
        if not self.enabled:
            self.enabled = True
            self.processor = MsgProcessor(handlers_list, encoding=self.__encoding)
        self._create_scan_consumer()

    def _update_offsets(self, offsets):
        for p, offset in offsets.items():
            self.consumer.seek(kafka.TopicPartition(self._topic, p), offset)

    def _init_offsets(self, batchsize):
        """
        Compute new initial and target offsets and do other maintenance tasks
        """
        upper_offsets = previous_lower_offsets = self._lower_offsets
        if not upper_offsets:
            upper_offsets = self.latest_offsets
        self._upper_offsets = {p: o for p, o in upper_offsets.items() if o > self._min_lower_offsets[p]}

        # remove db dupes not used anymore
        if self._dupes:
            for p in self._dupes.keys():
                if p not in self._upper_offsets:
                    db = self._dupes.pop(p)
                    db.close()
                    os.remove(db.filename)

        partition_batchsize = 0
        if self._upper_offsets:
            partition_batchsize = max(int(batchsize * self.__scan_excess), batchsize)
            self._lower_offsets = self._upper_offsets.copy()
            total_offsets_run = 0
            for p in sorted(self._upper_offsets.keys()):
                # readjust partition_batchsize when a partition scan starts from latest offset
                if total_offsets_run > 0 and partition_batchsize > batchsize:
                    partition_batchsize = batchsize
                if partition_batchsize > 0:
                    self._lower_offsets[p] = max(self._upper_offsets[p] - partition_batchsize, self._min_lower_offsets[p])
                    offsets_run = self._upper_offsets[p] - self._lower_offsets[p]
                    total_offsets_run += offsets_run
                    partition_batchsize = partition_batchsize - offsets_run
                else:
                    break
            log.info("Offset run: %d", total_offsets_run)
            # create new consumer if partition list changes
            if previous_lower_offsets is not None and set(previous_lower_offsets.keys()) != set(self._lower_offsets):
                self._create_scan_consumer(self._lower_offsets.keys())

            # consumer must restart from newly computed lower offsets
            self._update_offsets(self._lower_offsets)
        log.info("Initial offsets for topic %s: %s", self._topic, repr(self._lower_offsets))
        log.info("Target offsets for topic %s: %s", self._topic, repr(self._upper_offsets))

        return batchsize

    def _get_position(self):
        offsets = {}
        for partition in self.consumer.assignment:
            offsets[partition.partition] = self.consumer.position(partition)
        return offsets

    def _init_batch(self, batchsize):
        return self._init_offsets(batchsize)

    @retry(wait_fixed=60000, retry_on_exception=retry_on_exception)
    def _create_scan_consumer(self, partitions=None):
        self.consumer = kafka.KafkaConsumer(
            bootstrap_servers=self._brokers,
            group_id=self._group,
            enable_auto_commit=False,
            consumer_timeout_ms=100,
            api_version=self._api_version,
        )
        partitions = partitions or []
        partitions = [kafka.TopicPartition(self._topic, p) for p in partitions]
        self.consumer.assign(partitions or self._partitions)
        self.processor.set_consumer(self.consumer)

    def _scan_topic_batch(self, partition_batchsize):

        if not self._lower_offsets: # there is nothing to process
            self.enabled = False
            return

        max_next_messages = min(partition_batchsize, self.__max_next_messages)
        messages = MessageCache(self._dupes is not None)
        read_batch_count = 0
        while self.enabled and self.are_there_batch_messages_to_process(len(messages)):
            for partition, offset, key, msg in self.processor.process(max_next_messages):
                self.__real_scanned_count += 1
                record = {'_key': key, 'partition': partition, 'offset': offset, 'message': msg}
                if offset < self._upper_offsets[partition]:
                    self.__scanned_count += 1
                    if key in messages or not self._record_is_dupe(partition, key):
                        if self.must_delete_record(record):
                            self.__deleted_count += 1
                            if messages.get(key, None) is not None:
                                self.__issued_count = max(self.__issued_count - 1, 0)
                                self.__dupes_count += 1
                        elif key not in messages:
                            self.__issued_count += 1
                        elif self.must_delete_record(messages[key]):
                            self.__issued_count += 1
                            self.__dupes_count += 1
                            self.__deleted_count = max(self.__deleted_count - 1, 0)
                        else:
                            self.__dupes_count += 1
                        messages.append(record)
                        read_batch_count += 1
                        if len(messages) == max_next_messages:
                            yield messages.values()
                            messages = MessageCache(self._dupes is not None)

                if self.__real_scanned_count % self.__logcount == 0:
                    self.stats_logger.log_stats('Last key: {} '.format(key))

                # keep control of scanned count if defined
                if self.__issued_count > 0 and self.__issued_count == self._count:
                    self.enabled = False
                    break
        if len(messages):
            yield messages.values()
        self.__scan_excess = partition_batchsize / read_batch_count if read_batch_count > 0 else self.__scan_excess * 2
        log.info("Last offsets for topic %s: %s", self._topic, repr(self._get_position()))

    def _record_is_dupe(self, partition, key):
        if self._dupes is None:
            return False
        if key not in self._dupes[partition]:
            self._dupes[partition][key] = ''
            return False
        self.__dupes_count += 1
        return True

    def _filter_deleted_records(self, batches):
        """
        Filter out deleted records
        """
        for batch in batches:
            for record in batch:
                if not self.must_delete_record(record):
                    yield record

    def _process_offsetmsgs(self, omsgs):
        for omsg in omsgs:
            yield self.process_offsetmsg(omsg)

    def get_new_batch(self):

        batchsize = self.__batchsize
        if self._count > 0:
            batchsize = min(self.__batchsize, self._count - self.__issued_count)

        pipeline = batchsize
        for processor in [self._init_batch,
                          self._scan_topic_batch,
                          self._filter_deleted_records,
                          self.processor.processor_handlers.decompress_messages,
                          self.processor.processor_handlers.unpack_messages,
                          self._process_offsetmsgs]:
            pipeline = processor(pipeline)

        return pipeline

    def must_delete_record(self, record):
        return not record['message'] and not self._nodelete

    def end_batch_commit(self):
        pass

    def scan_topic_batches(self):
        self.init_scanner()
        records = MessageCache(False) # we don't need to dedupe here
        while self.enabled:
            if self.are_there_messages_to_process():
                for message in self.get_new_batch():
                    self.__last_message = message
                    if self.__batchcount > 0 and self.__issued_batches == self.__batchcount - 1:
                        self.enabled = False
                    if len(records) == self.__batchsize:
                        if self.__batch_autocommit:
                            self.end_batch_commit()
                        yield records.values()
                        records = MessageCache(False)
                        self.__issued_batches += 1
                    records.append(message['record'])
            else:
                break
        if records:
            yield records.values()
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
                self.consumer.close()
            if self._dupes is not None:
                for db in self._dupes.values():
                    db.close()
                shutil.rmtree(self._dupestempdir)

    @property
    def is_closed(self):
        return self.__closed

    def commit_final_offsets(self):
        pass

    def run(self):
        """ Convenient method for iterating along topic. """

        for batch in self.scan_topic_batches():
            for _ in batch:
                pass

        return self.issued_count

    def process_record(self, record):
        return record

    def process_offsetmsg(self, omsg):
        record = omsg.setdefault('record', {})
        record['_key'] = omsg['_key']
        record = self.process_record(record)
        return omsg 

    def are_there_messages_to_process(self):
        if self._lower_offsets is None:
            return True
        for partition, offset in self._lower_offsets.items():
            if offset > self._min_lower_offsets[partition]:
                return True
        return False

    def are_there_batch_messages_to_process(self, msgslen):
        for partition, offset in self._upper_offsets.items():
            if self._get_position()[partition] < offset:
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
    @retry(wait_fixed=60000, retry_on_exception=retry_on_exception)
    def latest_offsets(self):
        if not self._latest_offsets:
            consumer = kafka.KafkaConsumer(bootstrap_servers=self._brokers, group_id=None)
            partitions = [kafka.TopicPartition(self._topic, p.partition) for p in self._partitions]
            consumer.assign(partitions)
            self._latest_offsets = {p.partition: consumer.position(p) for p in partitions}
            consumer.close()
        return self._latest_offsets

    @property
    def partitions(self):
        return [p.partition for p in self._partitions]

    @property
    def topic(self):
        return self._topic

    @property
    def last_message(self):
        return self.__last_message

    def __iter__(self):
        self.__iter_batches = self.__iter_batches or self.scan_topic_batches()
        return self

    def next(self):
        return next(self.__iter_batches)

# for backward compatibility
KafkaScannerSimple = KafkaScanner


class KafkaScannerDirect(KafkaScannerSimple):
    """
    Scanner in direct sense. Dedupe is not supported in order to conserve
    logic of direct scanning. Also, delete records are issued.

    This is essentially a wrapper around KafkaConsumer for supporting same api than other scanner,
    with few extra feature support)

    start_offsets - allow to set start offsets dict.
    api_version - see kafka.consumer.group.KafkaConsumer docstring. Default here is (0,8,1) for
                  compatibility with previous scanner (commited offsets saved on zookeeper server)

    The rest of parameters has the same functionality as parent class
    """
    def __init__(self, brokers, topic, group, batchsize=DEFAULT_BATCH_SIZE, batchcount=0, keep_offsets=False,
            partitions=None, start_offsets=None, max_next_messages=10000, logcount=10000, batch_autocommit=True,
            api_version=(0,8,1)):
        super(KafkaScannerDirect, self).__init__(brokers, topic, group, batchsize=batchsize,
                    count=0, batchcount=batchcount, keep_offsets=keep_offsets, nodelete=True, nodedupe=True,
                    partitions=partitions, max_next_messages=max_next_messages, logcount=logcount, batch_autocommit=batch_autocommit)
        self._lower_offsets = start_offsets
        self._api_version = api_version
        self._init_consumer = None
        self._client = _get_client(self._brokers)

    @retry(wait_fixed=60000, retry_on_exception=retry_on_exception)
    def _create_init_consumer(self):
        return kafka.SimpleConsumer(self._client, self._group, self._topic, partitions=self.partitions)

    def init_scanner(self):
        super(KafkaScannerDirect, self).init_scanner()
        self._upper_offsets = self.latest_offsets
        if not self._group or not self._keep_offsets or self._lower_offsets is not None:
            if self._lower_offsets is None:
                self._lower_offsets = {partition: 0 for partition in self.init_consumer.offsets}
        else:
            self._lower_offsets = self.init_consumer.offsets.copy()
        for p, o in self._lower_offsets.items():
            if self._upper_offsets[p] < o:
                self._lower_offsets[p] = 0
                log.warning('Set lower offset for partition %d to 0', p)
        self.init_consumer.offsets.update(self._lower_offsets)
        self.init_consumer.count_since_commit += 1
        self.init_consumer.commit()
        self._create_scan_consumer()
        log.info("Initial offsets for topic %s: %s", self._topic, repr(self._get_position()))
        log.info("Target offsets for topic %s: %s", self._topic, repr(self._upper_offsets))

    def _set_consumer_group(self, group, keep_offsets):
        if isinstance(group, basestring):
            self._group = group
        if keep_offsets:
            assert self._group, 'keep_offsets option needs a group name'
        self._keep_offsets = keep_offsets

    def _init_offsets(self, batchsize):
        self._lower_offsets = self._get_position().copy()
        return batchsize // len(self._upper_offsets) or 1

    def _init_batch(self, batchsize):
        return self._init_offsets(batchsize)

    def end_batch_commit(self):
        if self.last_message:
            commit_offsets = self._lower_offsets.copy()
            commit_offsets[self.last_message['partition']] = self.last_message['offset']
            self._commit_offsets(commit_offsets)

    def commit_final_offsets(self):
        commit_offsets = self._get_position()
        self._commit_offsets(commit_offsets)

    def are_there_messages_to_process(self):
        for partition, offset in self._get_position().items():
            if offset < self.latest_offsets[partition]:
                return True
        return False

    def are_there_batch_messages_to_process(self, msgslen):
        if msgslen > self.batchsize:
            return False
        return super(KafkaScannerDirect, self).are_there_batch_messages_to_process(msgslen)

    @retry(wait_fixed=60000, retry_on_exception=retry_on_exception)
    def get_committed_offsets(self):
        consumer = kafka.KafkaConsumer(bootstrap_servers=self._brokers, group_id=self._group, api_version=self._api_version)
        consumer.assign(partitions)
        offsets = {p.partition: consumer.committed(p) or 0 for p in self._partitions}
        consumer.close()
        return offsets

    def reset_offsets(self, offsets=None):
        commit_offsets = offsets or {p: 0 for p in self.partitions or self.latest_offsets.keys()}
        self._commit_offsets(commit_offsets)

    @property
    def init_consumer(self):
        if self._init_consumer is None:
            self._init_consumer = self._create_init_consumer()
        return self._init_consumer

    @retry(wait_fixed=60000, retry_on_exception=retry_on_exception)
    def _commit_offsets(self, offsets):
        log.info('Commiting offsets for group %s (topic %s): %s', self._group, self._topic, offsets)
        self.init_consumer.offsets.update(offsets)
        self.init_consumer.count_since_commit += 1
        self.init_consumer.commit()

    def close(self):
        if not self.is_closed and self._init_consumer is not None:
            self._init_consumer.stop()
            self._init_consumer.client.close()
        super(KafkaScannerDirect, self).close()
