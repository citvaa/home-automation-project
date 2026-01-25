import threading
import time


def dus1_callback(distance):
    t = time.strftime("%H:%M:%S", time.localtime())
    print("=" * 20)
    if distance is None:
        print(f"[DUS1 Ultrasonic] {t} distance: timeout")
    else:
        print(f"[DUS1 Ultrasonic] {t} distance: {distance:.2f} cm")


def run_dus1(settings, threads, stop_event, callback=None):
    if settings is None:
        return

    if callback is None:
        callback = dus1_callback

    delay = settings.get("delay", 10.0)
    trig = settings.get("trig")
    echo = settings.get("echo")

    if settings.get("simulated", False):
        from app.sim.dus1 import run_dus1_simulator

        print("Starting DUS1 simulator")
        thread = threading.Thread(
            target=run_dus1_simulator,
            args=(delay, callback, stop_event),
            daemon=True,
        )
        thread.start()
        threads.append(thread)
        print("DUS1 simulator started")
    else:
        try:
            from app.hw.dus1 import run_dus1_loop
        except ImportError:
            print("RPi.GPIO not available; cannot start DUS1 real loop.")
            return

        if trig is None or echo is None:
            print("DUS1 pins not configured; skipping.")
            return

        print("Starting DUS1 real loop")
        thread = threading.Thread(
            target=run_dus1_loop,
            args=(trig, echo, delay, callback, stop_event),
            daemon=True,
        )
        thread.start()
        threads.append(thread)
        print("DUS1 loop started")
