import time
import RPi.GPIO as GPIO


def setup_membrane(pin):
    GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)


def run_dms_loop(pin, delay, callback, stop_event):
    setup_membrane(pin)
    last_state = None
    while not stop_event.is_set():
        state = GPIO.input(pin) == GPIO.LOW
        if state != last_state:
            callback(state)
            last_state = state
        time.sleep(delay)
