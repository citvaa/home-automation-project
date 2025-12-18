import time
import RPi.GPIO as GPIO


def setup_pir(pin):
    GPIO.setup(pin, GPIO.IN)


def run_pir_loop(pin, delay, callback, stop_event):
    setup_pir(pin)
    last_state = None
    while not stop_event.is_set():
        state = GPIO.input(pin) == GPIO.HIGH
        if state != last_state:
            callback(state)
            last_state = state
        time.sleep(delay)
