# Ensure running the module directly (python app/main.py) works by adding project root to sys.path
# Prefer running with: python -m app.main
import sys, pathlib
if __package__ is None:
    sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

import threading
import time
from datetime import datetime, timezone
from typing import Optional
from app.config.settings import load_settings, load_config

# Sensors/components
from app.components.ds1 import run_ds1
from app.components.pir import run_pir
from app.components.dus1 import run_dus1
from app.components.dms import run_dms
from app.components.dl import DLController
from app.components.db import DBController
from app.components.mqtt_publisher import BatchPublisher

# RPi.GPIO is optional (not needed for simulation)
try:
    import RPi.GPIO as GPIO
    GPIO.setmode(GPIO.BCM)
except ImportError:
    GPIO = None


def start_ds1(settings, threads, stop_event, publisher: Optional[BatchPublisher] = None):
    if settings is None:
        return

    def cb(state):
        t = time.strftime("%H:%M:%S", time.localtime())
        print("=" * 20)
        print(f"[DS1 Door Sensor] {t} state: {'PRESSED' if state else 'RELEASED'}")
        if publisher:
            reading = {
                "sensor_type": "DS1",
                "value": state,
                "unit": None,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "simulated": settings.get("simulated", False),
            }
            publisher.enqueue_reading(reading)

    run_ds1(settings, threads, stop_event, callback=cb)


def start_pir(settings, threads, stop_event, publisher: Optional[BatchPublisher] = None):
    if settings is None:
        return

    def cb(state):
        t = time.strftime("%H:%M:%S", time.localtime())
        print("=" * 20)
        print(f"[DPIR1 Motion Sensor] {t} motion: {'DETECTED' if state else 'CLEAR'}")
        if publisher:
            reading = {
                "sensor_type": "DPIR1",
                "value": state,
                "unit": None,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "simulated": settings.get("simulated", False),
            }
            publisher.enqueue_reading(reading)

    run_pir(settings, threads, stop_event, callback=cb)


def start_dus1(settings, threads, stop_event, publisher: Optional[BatchPublisher] = None):
    if settings is None:
        return

    def cb(distance):
        t = time.strftime("%H:%M:%S", time.localtime())
        print("=" * 20)
        if distance is None:
            print(f"[DUS1 Ultrasonic] {t} distance: timeout")
        else:
            print(f"[DUS1 Ultrasonic] {t} distance: {distance:.2f} cm")
        if publisher:
            reading = {
                "sensor_type": "DUS1",
                "value": distance,
                "unit": "cm",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "simulated": settings.get("simulated", False),
            }
            publisher.enqueue_reading(reading)

    run_dus1(settings, threads, stop_event, callback=cb)


def start_dms(settings, threads, stop_event, publisher: Optional[BatchPublisher] = None):
    if settings is None:
        return

    def cb(state):
        t = time.strftime("%H:%M:%S", time.localtime())
        print("=" * 20)
        print(f"[DMS Membrane Switch] {t} state: {'PRESSED' if state else 'RELEASED'}")
        if publisher:
            reading = {
                "sensor_type": "DMS",
                "value": state,
                "unit": None,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "simulated": settings.get("simulated", False),
            }
            publisher.enqueue_reading(reading)

    run_dms(settings, threads, stop_event, callback=cb)


def command_loop(stop_event, led_ctrl=None, buzzer_ctrl=None):
    """
    Console loop for actuator control (LED, buzzer).
    Commands:
      - led         -> toggle LED (if off, turn on; if on, turn off)
      - buzzer      -> toggle buzzer
      - led on/off  -> explicit on/off
      - buzzer on/off
      - quit/exit/q
    """
    led_state = False
    buzzer_state = False

    while not stop_event.is_set():
        try:
            cmd = input("cmd (led | buzzer | quit): ").strip().lower()
        except EOFError:
            # stdin closed (e.g., in test environment)
            break

        if cmd in ("q", "quit", "exit"):
            stop_event.set()
            break
        elif cmd in ("led", "led on", "led off"):
            if cmd == "led":
                led_state = not led_state
            else:
                led_state = cmd.endswith("on")

            if led_ctrl:
                led_ctrl.set_state(led_state)
            else:
                print("LED controller not available.")
        elif cmd in ("buzzer", "buzzer on", "buzzer off"):
            if cmd == "buzzer":
                buzzer_state = not buzzer_state
            else:
                buzzer_state = cmd.endswith("on")

            if buzzer_ctrl:
                buzzer_ctrl.set_state(buzzer_state)
            else:
                print("Buzzer controller not available.")
        elif cmd == "":
            continue
        else:
            print("Nepoznata komanda.")


if __name__ == "__main__":
    print("Starting app")
    raw_settings = load_settings()
    cfg = load_config()
    threads = []
    stop_event = threading.Event()

    # Start publisher
    publisher = BatchPublisher(cfg)
    publisher.start()

    try:
        start_ds1(raw_settings.get("DS1"), threads, stop_event, publisher=publisher)
        start_pir(raw_settings.get("DPIR1"), threads, stop_event, publisher=publisher)
        start_dus1(raw_settings.get("DUS1"), threads, stop_event, publisher=publisher)
        start_dms(raw_settings.get("DMS"), threads, stop_event, publisher=publisher)

        led_ctrl = DLController(raw_settings.get("DL"))
        buzzer_ctrl = DBController(raw_settings.get("DB"))

        # Start command loop for actuators in a separate thread
        cmd_thread = threading.Thread(target=command_loop, args=(stop_event, led_ctrl, buzzer_ctrl), daemon=True)
        cmd_thread.start()
        threads.append(cmd_thread)

        while not stop_event.is_set():
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("Stopping app")
        stop_event.set()
    finally:
        # Stop publisher and join threads
        try:
            publisher.stop()
        except Exception:
            pass
        for t in threads:
            t.join(timeout=1)
