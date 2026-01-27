import threading
import time
from common.logging import get_logger
logger = get_logger(__name__)


def ds1_callback(state):
    t = time.strftime("%H:%M:%S", time.localtime())
    logger.info("[DS1 Door Sensor] %s state: %s", t, 'PRESSED' if state else 'RELEASED')


def run_ds1(settings, threads, stop_event, callback=None):
    if settings is None:
        return

    if callback is None:
        callback = ds1_callback

    delay = settings.get("delay", 10.0)

    if settings.get("simulated", False):
        from edge.sim.ds1 import run_ds1_simulator

        logger.info("Starting DS1 simulator")
        thread = threading.Thread(target=run_ds1_simulator, args=(delay, callback, stop_event), daemon=True)
        thread.start()
        threads.append(thread)
        logger.info("DS1 simulator started")
    else:
        try:
            from edge.hw.ds1 import run_ds1_loop
        except ImportError:
            logger.warning("RPi.GPIO not available; cannot start DS1 real loop.")
            return

        print("Starting DS1 real loop")
        thread = threading.Thread(
            target=run_ds1_loop, args=(settings["pin"], delay, callback, stop_event), daemon=True
        )
        thread.start()
        threads.append(thread)
        print("DS1 loop started")
