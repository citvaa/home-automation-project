try:
    import RPi.GPIO as GPIO
except Exception:
    GPIO = None


def setup_led(pin):
    if GPIO is not None:
        GPIO.setup(pin, GPIO.OUT)
        GPIO.output(pin, GPIO.LOW)


def set_led(pin, on):
    if GPIO is not None:
        GPIO.output(pin, GPIO.HIGH if on else GPIO.LOW)
