from tj_worker import swap_getter, swap_etl
from tj_worker.utils import settings, log

from time import sleep

logger = log.setup_custom_logger(name=__file__)

if settings.SLEEP_MODE is not None:
    while True:
        logger.info("Sleep Mode Activated")
        sleep(300)

if settings.MODULE_TO_RUN == "swap_getter":
    swap_getter.run()
elif settings.MODULE_TO_RUN == "swap_etl":
    swap_etl.run()
