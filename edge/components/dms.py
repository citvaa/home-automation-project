import threading
import time
from common.logging import get_logger
logger = get_logger(__name__)


def dms_callback(state):
    t = time.strftime("%H:%M:%S", time.localtime())
    logger.info("[DMS Membrane Switch] %s state: %s", t, 'PRESSED' if state else 'RELEASED')


def run_dms(settings, threads, stop_event, callback=None):
    if settings is None:
        return

    if callback is None:
        callback = dms_callback

    delay = settings.get("delay", 10.0)

    if settings.get("simulated", False):
        from edge.sim.dms import run_dms_simulator

        logger.info("Starting DMS simulator")
        thread = threading.Thread(target=run_dms_simulator, args=(delay, callback, stop_event), daemon=True)
        thread.start()
        threads.append(thread)
        logger.info("DMS simulator started")
    else:
        try:
            from edge.hw.dms import run_dms_loop
        except ImportError:
            logger.warning("RPi.GPIO not available; cannot start DMS real loop.")
            return

        print("Starting DMS real loop")
        thread = threading.Thread(target=run_dms_loop, args=(settings["pin"], delay, callback, stop_event), daemon=True)
        thread.start()
        threads.append(thread)
        print("DMS loop started")
