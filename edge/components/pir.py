import threading
import time


def pir_callback(state):
    t = time.strftime("%H:%M:%S", time.localtime())
    print("=" * 20)
    print(f"[DPIR1 Motion Sensor] {t} motion: {'DETECTED' if state else 'CLEAR'}")


def run_pir(settings, threads, stop_event, callback=None):
    if settings is None:
        return

    if callback is None:
        callback = pir_callback

    delay = settings.get("delay", 10.0)

    if settings.get("simulated", False):
        from edge.sim.pir import run_pir_simulator

        print("Starting PIR simulator")
        thread = threading.Thread(target=run_pir_simulator, args=(delay, callback, stop_event), daemon=True)
        thread.start()
        threads.append(thread)
        print("PIR simulator started")
    else:
        try:
            from edge.hw.pir import run_pir_loop
        except ImportError:
            print("RPi.GPIO not available; cannot start PIR real loop.")
            return

        print("Starting PIR real loop")
        thread = threading.Thread(target=run_pir_loop, args=(settings["pin"], delay, callback, stop_event), daemon=True)
        thread.start()
        threads.append(thread)
        print("PIR loop started")
