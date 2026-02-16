import time
try:
    import RPi.GPIO as GPIO
except Exception:
    GPIO = None


def setup_button(pin):
    if GPIO is not None:
        GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)


def run_ds1_loop(pin, delay, callback, stop_event):
    setup_button(pin)
    last_state = None
    while not stop_event.is_set():
        state = GPIO is not None and GPIO.input(pin) == GPIO.LOW  # pressed when pulled low
        if state != last_state:
            callback(state)
            last_state = state
        time.sleep(delay)
