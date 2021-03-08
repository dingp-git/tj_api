import logging


def get_logger(name=None):
    return logging.getLogger('%s' % name)
