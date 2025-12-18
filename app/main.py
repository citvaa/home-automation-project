import threading
import time
from app.config.settings import load_settings

# Sensors/components
from app.components.ds1 import run_ds1
from app.components.pir import run_pir
from app.components.dus1 import run_dus1
from app.components.dms import run_dms
from app.components.dl import DLController
from app.components.db import DBController

# RPi.GPIO is optional (not needed for simulation)
try:
    import RPi.GPIO as GPIO
    GPIO.setmode(GPIO.BCM)
except ImportError:
    GPIO = None


def start_ds1(settings, threads, stop_event):
    if settings is None:
        return
    run_ds1(settings, threads, stop_event)


def start_pir(settings, threads, stop_event):
    if settings is None:
        return
    run_pir(settings, threads, stop_event)


def start_dus1(settings, threads, stop_event):
    if settings is None:
        return
    run_dus1(settings, threads, stop_event)


def start_dms(settings, threads, stop_event):
    if settings is None:
        return
    run_dms(settings, threads, stop_event)


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
    settings = load_settings()
    threads = []
    stop_event = threading.Event()

    try:
        start_ds1(settings.get("DS1"), threads, stop_event)
        start_pir(settings.get("DPIR1"), threads, stop_event)
        start_dus1(settings.get("DUS1"), threads, stop_event)
        start_dms(settings.get("DMS"), threads, stop_event)

        led_ctrl = DLController(settings.get("DL"))
        buzzer_ctrl = DBController(settings.get("DB"))

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
        for t in threads:
            t.join(timeout=1)
