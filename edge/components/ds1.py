import threading
import time


def ds1_callback(state):
    t = time.strftime("%H:%M:%S", time.localtime())
    print("=" * 20)
    print(f"[DS1 Door Sensor] {t} state: {'PRESSED' if state else 'RELEASED'}")


def run_ds1(settings, threads, stop_event, callback=None):
    if settings is None:
        return

    if callback is None:
        callback = ds1_callback

    delay = settings.get("delay", 10.0)

    if settings.get("simulated", False):
        from edge.sim.ds1 import run_ds1_simulator

        print("Starting DS1 simulator")
        thread = threading.Thread(target=run_ds1_simulator, args=(delay, callback, stop_event), daemon=True)
        thread.start()
        threads.append(thread)
        print("DS1 simulator started")
    else:
        try:
            from edge.hw.ds1 import run_ds1_loop
        except ImportError:
            print("RPi.GPIO not available; cannot start DS1 real loop.")
            return

        print("Starting DS1 real loop")
        thread = threading.Thread(
            target=run_ds1_loop, args=(settings["pin"], delay, callback, stop_event), daemon=True
        )
        thread.start()
        threads.append(thread)
        print("DS1 loop started")
