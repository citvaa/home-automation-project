import threading
import time


def dms_callback(state):
    t = time.strftime("%H:%M:%S", time.localtime())
    print("=" * 20)
    print(f"[DMS Membrane Switch] {t} state: {'PRESSED' if state else 'RELEASED'}")


def run_dms(settings, threads, stop_event, callback=None):
    if settings is None:
        return

    if callback is None:
        callback = dms_callback

    delay = settings.get("delay", 10.0)

    if settings.get("simulated", False):
        from edge.sim.dms import run_dms_simulator

        print("Starting DMS simulator")
        thread = threading.Thread(target=run_dms_simulator, args=(delay, callback, stop_event), daemon=True)
        thread.start()
        threads.append(thread)
        print("DMS simulator started")
    else:
        try:
            from edge.hw.dms import run_dms_loop
        except ImportError:
            print("RPi.GPIO not available; cannot start DMS real loop.")
            return

        print("Starting DMS real loop")
        thread = threading.Thread(target=run_dms_loop, args=(settings["pin"], delay, callback, stop_event), daemon=True)
        thread.start()
        threads.append(thread)
        print("DMS loop started")
