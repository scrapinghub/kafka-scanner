import zlib
import time
import logging
import traceback

import msgpack
from retrying import retry

from .exceptions import TestException, NoDataException

log = logging.getLogger(__name__)


def retry_on_exception(exception):
    log.error(traceback.format_exc())
    for etype in (KeyboardInterrupt, TestException):
        if isinstance(exception, etype):
            return False
    return True


class MsgProcessorHandlers(object):
    def __init__(self, encoding=None):
        self.decompress_fun = zlib.decompress
        self.consumer = None
        self.__next_messages = 0
        self.__encoding = encoding

    def set_consumer(self, consumer):
        self.consumer = consumer

    def set_next_messages(self, next_messages):
        if next_messages != self.__next_messages:
            self.__next_messages = next_messages
            log.info('Next messages count adjusted to {}'.format(next_messages))

    @property
    def next_messages(self):
        return self.__next_messages

    @retry(wait_fixed=60000, retry_on_exception=retry_on_exception)
    def _get_message_from_consumer(self):
        for m in self.consumer:
            return m

    def _get_messages_from_consumer(self):
        count = 0
        while True:
            m = self._get_message_from_consumer()
            if m:
                yield m
                count += 1
                if count == self.__next_messages:
                    break
            else:
                break

    def consume_messages(self, max_next_messages):
        """ Get messages batch from Kafka (list at output) """
        # get messages list from kafka
        if self.__next_messages == 0:
            self.set_next_messages(min(1000, max_next_messages))
        self.set_next_messages(min(self.__next_messages, max_next_messages))
        mark = time.time()
        for record in self._get_messages_from_consumer():
            yield record.partition, record.offset, record.key, record.value
        newmark = time.time()
        if newmark - mark > 30:
            self.set_next_messages(self.__next_messages / 2 or 1)
        elif newmark - mark < 5:
            self.set_next_messages(min(self.__next_messages + 100, max_next_messages))

    def decompress_messages(self, partitions_offmsgs):
        """ Decompress pre-defined compressed fields for each message. """

        for pomsg in partitions_offmsgs:
            if pomsg['message']:
                pomsg['message'] = self.decompress_fun(pomsg['message'])
            yield pomsg

    def unpack_messages(self, partitions_msgs):
        """ Deserialize a message to python structures """

        for pmsg in partitions_msgs:
            key = pmsg['_key']
            partition = pmsg['partition']
            offset = pmsg['offset']
            msg = pmsg.pop('message')
            if msg:
                try:
                    record = msgpack.unpackb(msg, encoding=self.__encoding)
                except Exception as e:
                    log.error("Error unpacking record at partition:offset {}:{} (key: {} : {})".format(partition, offset, key, repr(e)))
                    continue
                else:
                    if isinstance(record, dict):
                        pmsg['record'] = record
                        yield pmsg
                    else:
                        log.info('Record {} has wrong type'.format(key))
            else:
                yield pmsg
