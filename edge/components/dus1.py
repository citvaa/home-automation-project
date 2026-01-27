import threading
import time
from common.logging import get_logger
logger = get_logger(__name__)


def dus1_callback(distance):
    t = time.strftime("%H:%M:%S", time.localtime())
    if distance is None:
        logger.info("[DUS1 Ultrasonic] %s distance: timeout", t)
    else:
        logger.info("[DUS1 Ultrasonic] %s distance: %.2f cm", t, distance)


def run_dus1(settings, threads, stop_event, callback=None):
    if settings is None:
        return

    if callback is None:
        callback = dus1_callback

    delay = settings.get("delay", 10.0)

    if settings.get("simulated", False):
        from edge.sim.dus1 import run_dus1_simulator

        logger.info("Starting DUS1 simulator")
        thread = threading.Thread(target=run_dus1_simulator, args=(delay, callback, stop_event), daemon=True)
        thread.start()
        threads.append(thread)
        logger.info("DUS1 simulator started")
    else:
        try:
            from edge.hw.dus1 import run_dus1_loop
        except ImportError:
            logger.warning("RPi.GPIO not available; cannot start DUS1 real loop.")
            return

        print("Starting DUS1 real loop")
        thread = threading.Thread(
            target=run_dus1_loop, args=(settings["trig"], settings["echo"], delay, callback, stop_event), daemon=True
        )
        thread.start()
        threads.append(thread)
        print("DUS1 loop started")
