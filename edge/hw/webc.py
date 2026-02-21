import time
from common.logging import get_logger

logger = get_logger(__name__)


def run_webc_loop(settings, delay, callback, stop_event):
    # Placeholder for real camera integration
    logger.warning("WEBC real camera loop not implemented; stopping.")
    while not stop_event.is_set():
        time.sleep(delay)
