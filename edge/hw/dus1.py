import time
try:
    import RPi.GPIO as GPIO
except Exception:
    GPIO = None


def setup_ultrasonic(trig_pin, echo_pin):
    if GPIO is not None:
        GPIO.setup(trig_pin, GPIO.OUT)
        GPIO.setup(echo_pin, GPIO.IN)
        GPIO.output(trig_pin, False)
        time.sleep(0.05)


def measure_distance(trig_pin, echo_pin, timeout=0.02):
    if GPIO is None:
        return None
    GPIO.output(trig_pin, True)
    time.sleep(0.00001)
    GPIO.output(trig_pin, False)

    start = time.time()
    while GPIO.input(echo_pin) == 0:
        if time.time() - start > timeout:
            return None
    pulse_start = time.time()

    while GPIO.input(echo_pin) == 1:
        if time.time() - pulse_start > timeout:
            return None
    pulse_end = time.time()

    pulse_duration = pulse_end - pulse_start
    distance = (pulse_duration * 34300) / 2
    return distance


def run_dus1_loop(trig_pin, echo_pin, delay, callback, stop_event):
    setup_ultrasonic(trig_pin, echo_pin)
    while not stop_event.is_set():
        dist = measure_distance(trig_pin, echo_pin)
        callback(dist)
        time.sleep(delay)
