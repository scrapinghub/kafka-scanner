"""
MultiProcessConsumer extender with partition info per message
"""
import time

from retrying import retry
from kafka import MultiProcessConsumer

from .utils import retry_on_exception

try:
    from Queue import Empty  # python 3
except ImportError:
    from queue import Empty  # python 2


class ExtendedMultiProcessConsumer(MultiProcessConsumer):
    def __init__(self, *args, **kwargs):
        super(ExtendedMultiProcessConsumer, self).__init__(*args, **kwargs)
        self.partition_info = False     # Do not return partition info in msgs

    def provide_partition_info(self):
        """
        Indicates that partition info must be returned by the consumer
        """
        self.partition_info = True

    @retry(wait_fixed=60000, retry_on_exception=retry_on_exception)
    def get_messages(self, count=1, block=True, timeout=10):
        """
        Fetch the specified number of messages

        Keyword Arguments:
            count: Indicates the maximum number of messages to be fetched
            block: If True, the API will block till some messages are fetched.
            timeout: If block is True, the function will block for the specified
                time (in seconds) until count messages is fetched. If None,
                it will block forever.
        """
        messages = []

        # Give a size hint to the consumers. Each consumer process will fetch
        # a maximum of "count" messages. This will fetch more messages than
        # necessary, but these will not be committed to kafka. Also, the extra
        # messages can be provided in subsequent runs
        self.size.value = count
        self.events.pause.clear()

        if timeout is not None:
            max_time = time.time() + timeout

        new_offsets = {}
        while count > 0 and (timeout is None or timeout > 0):
            # Trigger consumption only if the queue is empty
            # By doing this, we will ensure that consumers do not
            # go into overdrive and keep consuming thousands of
            # messages when the user might need only a few
            if self.queue.empty():
                self.events.start.set()

            try:
                partition, message = self.queue.get(block, timeout)
            except Empty:
                break

            _msg = (partition, message) if self.partition_info else message
            messages.append(_msg)
            new_offsets[partition] = message.offset + 1
            count -= 1
            if timeout is not None:
                timeout = max_time - time.time()

        self.size.value = 0
        self.events.start.clear()
        self.events.pause.set()

        # Update and commit offsets if necessary
        self.offsets.update(new_offsets)
        self.count_since_commit += len(messages)
        self._auto_commit()

        return messages
