import logging

from datetime import datetime


class Logger:

    def __init__(self, ln=''):
        # init logging
        now = datetime.now()
        logging.basicConfig(filename='logs/%s_%s_%s_%s.log' % (ln, now.year, now.month, now.day, ), level=logging.INFO)

    def record(self, msg, lt='INFO'):
        '''
        DEBUG   Detailed information, typically of interest only when diagnosing problems.
        INFO    Confirmation that things are working as expected.
        WARNING An indication that something unexpected happened, or indicative of some problem in the near future (e.g. ‘disk space low’). The software is still working as expected.
        ERROR   Due to a more serious problem, the software has not been able to perform some function.
        CRITICAL    A serious error, indicating that the program itself may be unable to continue running.
        '''
        FORMAT_MSG = """\nDATETIME:{0}\nMESSAGE:{1}\n""".format(datetime.now(), msg)

        if 'DEBUG' == lt:
            logging.debug(FORMAT_MSG)
        elif 'WARNING' == lt:
            logging.warning(FORMAT_MSG)
        elif 'ERROR' == lt:
            logging.error(FORMAT_MSG)
        elif 'CRITICAL' == lt:
            logging.caitical(FORMAT_MSG)
        else:
            logging.info(FORMAT_MSG)
