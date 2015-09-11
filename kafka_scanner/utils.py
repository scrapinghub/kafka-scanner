import traceback

def retry_on_exception(exception):
    print "Retried: {}".format(traceback.format_exc())
    return not isinstance(exception, KeyboardInterrupt)
