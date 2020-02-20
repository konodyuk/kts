import logging

from kts.settings import cfg

logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%H:%M:%S',
    level=logging.DEBUG)

logger = logging.getLogger(__name__)


if not cfg.debug:
    logger.level = 50


debug = logger.debug
exc = logger.exception
