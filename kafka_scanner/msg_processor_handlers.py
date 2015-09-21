import msgpack
import zlib
import time
import logging


log = logging.getLogger(__name__)


class MsgProcessorHandlers(object):
    def __init__(self):
        self.decompress_fun = zlib.decompress
        self.consumer = None
        self.__next_messages = 0

    def set_consumer(self, consumer):
        self.consumer = consumer

    def set_next_messages(self, next_messages):
        if next_messages != self.__next_messages:
            self.__next_messages = next_messages
            log.info('Next messages count adjusted to {}'.format(next_messages))

    @property
    def next_messages(self):
        return self.__next_messages
        
    def consume_messages(self, max_next_messages):
        """ Get messages batch from Kafka (list at output) """
        # get messages list from kafka
        if self.__next_messages == 0:
            self.set_next_messages(1000)
        self.set_next_messages(min(self.__next_messages, max_next_messages))
        mark = time.time()
        for partition, offmsg in self.consumer.get_messages(self.__next_messages):
            yield partition, offmsg
        newmark = time.time()
        if newmark - mark > 30:
            self.set_next_messages(self.__next_messages / 2 or 1)
        elif newmark - mark < 5:
            self.set_next_messages(min(self.__next_messages + 100, max_next_messages))

    def decompress_messages(self, partitions_offmsgs):
        """ Decompress pre-defined compressed fields for each message. """

        for partition, offmsg in partitions_offmsgs:
            if offmsg.message.value:
                yield partition, offmsg.offset, offmsg.message.key, self.decompress_fun(offmsg.message.value)
            else:
                yield partition, offmsg.offset, offmsg.message.key, offmsg.message.value

    @staticmethod
    def unpack_messages(partitions_msgs):
        """ Deserialize a message to python structures """

        for partition, offset, key, msg in partitions_msgs:
            if msg:
                record = msgpack.unpackb(msg)
                if isinstance(record, dict):
                    record['_key'] = key
                    yield partition, offset, record
                else:
                    log.info('Record {} has wrong type'.format(key))
            else:
                yield partition, offset, {'_key': key}
