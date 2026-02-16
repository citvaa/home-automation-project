import time
try:
    import RPi.GPIO as GPIO
except Exception:
    GPIO = None


def setup_pir(pin):
    if GPIO is not None:
        GPIO.setup(pin, GPIO.IN)


def run_pir_loop(pin, delay, callback, stop_event):
    setup_pir(pin)
    last_state = None
    while not stop_event.is_set():
        state = GPIO is not None and GPIO.input(pin) == GPIO.HIGH
        if state != last_state:
            callback(state)
            last_state = state
        time.sleep(delay)
