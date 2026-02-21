import threading
import time
from common.logging import get_logger

logger = get_logger(__name__)


def webc_callback(payload):
    t = time.strftime("%H:%M:%S", time.localtime())
    if isinstance(payload, dict):
        mime = payload.get("mime", "unknown")
        size = payload.get("bytes", "?")
        logger.info("[WEBC Camera] %s frame: %s bytes (%s)", t, size, mime)
    else:
        logger.info("[WEBC Camera] %s frame received", t)


def run_webc(settings, threads, stop_event, callback=None):
    if settings is None:
        return

    if callback is None:
        callback = webc_callback

    delay = settings.get("delay", 2.0)

    if settings.get("simulated", False):
        from edge.sim.webc import run_webc_simulator

        frame_bytes = settings.get("frame_bytes", 512)
        logger.info("Starting WEBC simulator")
        thread = threading.Thread(
            target=run_webc_simulator,
            args=(delay, callback, stop_event, frame_bytes),
            daemon=True,
        )
        thread.start()
        threads.append(thread)
        logger.info("WEBC simulator started")
    else:
        try:
            from edge.hw.webc import run_webc_loop
        except ImportError:
            logger.warning("Camera hardware not available; cannot start WEBC real loop.")
            return

        logger.info("Starting WEBC real loop")
        thread = threading.Thread(
            target=run_webc_loop,
            args=(settings, delay, callback, stop_event),
            daemon=True,
        )
        thread.start()
        threads.append(thread)
        logger.info("WEBC loop started")
