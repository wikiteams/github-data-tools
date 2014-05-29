#####################################
# handy utility for managing output #
#    call me at once, if you can    #
#       if can't, also call me      #
#               S.H.                #
#####################################

import sys
import logging
import logging.handlers
import logging.config
import __builtin__

DISABLE__STD = False

logging.config.fileConfig('logging.conf')
logger = logging.getLogger(__name__)

intelliTag_verbose = False


def log(s):
    if intelliTag_verbose or __builtin__.verbose:
        logger.info(str(s))


def say(s):
    if intelliTag_verbose or __builtin__.verbose:
        print str(s)


# prints a message to console no matter what
def cout(s):
    print str(s)


# prints an error message to standard err output
def err(s):
    print str(s)


def ssay(s):
    if intelliTag_verbose or __builtin__.verbose:
        print str(s)
        logger.info(str(s))


def log_error(s):
    if intelliTag_verbose:
        logger.error(s)


def log_warning(s):
    if intelliTag_verbose:
        logger.warning(s)


def log_debug(s):
    if intelliTag_verbose:
        logger.debug(s)


def std_fwrite(s):
    sys.stdout.write(s)
    sys.stdout.flush()


def std_write(s):
    if (intelliTag_verbose) and (not DISABLE__STD):
        sys.stdout.write(s)
        sys.stdout.flush()
