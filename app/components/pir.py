import threading
import time


def pir_callback(state):
    t = time.strftime("%H:%M:%S", time.localtime())
    print("=" * 20)
    print(f"[DPIR1 Motion Sensor] {t} motion: {'DETECTED' if state else 'CLEAR'}")


def run_pir(settings, threads, stop_event):
    if settings is None:
        return

    delay = settings.get("delay", 10.0)

    if settings.get("simulated", False):
        from app.sim.pir import run_pir_simulator

        print("Starting DPIR1 simulator")
        thread = threading.Thread(target=run_pir_simulator, args=(delay, pir_callback, stop_event), daemon=True)
        thread.start()
        threads.append(thread)
        print("DPIR1 simulator started")
    else:
        try:
            from app.hw.pir import run_pir_loop
        except ImportError:
            print("RPi.GPIO not available; cannot start DPIR1 real loop.")
            return

        print("Starting DPIR1 real loop")
        thread = threading.Thread(
            target=run_pir_loop, args=(settings["pin"], delay, pir_callback, stop_event), daemon=True
        )
        thread.start()
        threads.append(thread)
        print("DPIR1 loop started")
