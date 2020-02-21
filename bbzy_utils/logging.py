import logging
import sys
from logging import Logger
from typing import TextIO


class ContextLogger:
    def __init__(self, logger: Logger, tag: str):
        self._tag = tag
        self._logger = logger

    def __enter__(self):
        self._logger.info(f'{self._tag} begin')

    def __exit__(self, *args):
        self._logger.info(f'{self._tag} end')


def init_logger(
        name,
        log_level=logging.INFO if not __debug__ else logging.DEBUG,
        *,
        log_name=False,
        log_process=False,
        log_thread=False,
        stream=None,
        clean_handlers=False,
):
    """
    :param str name:
    :param int log_level:
    :param bool log_name:
    :param bool log_process:
    :param bool log_thread:
    :param TextIO stream: None for stdout
    :param bool clean_handlers:
    :rtype: logging.Logger
    :return:
    """
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    log_format_parts = list()
    if log_process:
        log_format_parts.append('[%(process)d]')
    if log_thread:
        log_format_parts.append('%(threadName)s')
    log_format_parts.append('[%(asctime)s]')
    log_format_parts.append('[%(levelname)s]')
    if log_name:
        log_format_parts.append('[%(name)s]')
    log_format_parts.append('%(message)s')
    logger.propagate = False
    if stream is None:
        stream = sys.stdout
    handler = logging.StreamHandler(stream)
    formatter = logging.Formatter(' '.join(log_format_parts))
    handler.setFormatter(formatter)
    if clean_handlers:
        while logger.handlers:
            logger.removeHandler(logger.handlers[-1])
    logger.addHandler(handler)
    return logger
