from .msg_processor_handlers import MsgProcessorHandlers

class MsgProcessor(object):

    def __init__(self, handlers_list, encoding=None):
        self._handlers = []
        self.processor_handlers = MsgProcessorHandlers(encoding=encoding)
        for handler_name in handlers_list:
            self.add_handler(getattr(self.processor_handlers, handler_name))

    def set_consumer(self, consumer):
        self.processor_handlers.set_consumer(consumer)

    def add_handler(self, new_handler):
        if callable(new_handler):
            self._handlers.append(new_handler)

    def process(self, args):
        pipeline = args
        for new_handler in self._handlers:
            pipeline = new_handler(pipeline)
        return pipeline
