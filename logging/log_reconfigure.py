#!/usr/bin/env python3

import logging
import os
import tempfile


def log_reconfigure():
    log_file = os.path.join(tempfile.gettempdir(), 'log_reconfigure1.log')
    logging.basicConfig(level=logging.DEBUG, filename=log_file, format='%(message)s')

    logger = logging.getLogger(__file__)
    logger.info('file: %s', log_file)

    # reconfigure: not working
    log_file = os.path.join(tempfile.gettempdir(), 'log_reconfigure2.log')
    logging.basicConfig(level=logging.DEBUG, filename=log_file)

    logger.info('file failed: %s', log_file)

    # reconfigure: not working
    logging.shutdown()
    log_file = os.path.join(tempfile.gettempdir(), 'log_reconfigure3.log')
    logging.basicConfig(level=logging.DEBUG, filename=log_file)

    logger.info('file failed: %s', log_file)

    # reconfigure: working
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    log_file = os.path.join(tempfile.gettempdir(), 'log_reconfigure4.log')
    logging.basicConfig(level=logging.DEBUG, filename=log_file, format='[%(levelname)s] %(message)s')

    logger.info('file success: %s', log_file)


if __name__ == '__main__':
    log_reconfigure()
