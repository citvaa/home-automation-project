import RPi.GPIO as GPIO


def setup_led(pin):
    GPIO.setup(pin, GPIO.OUT)
    GPIO.output(pin, GPIO.LOW)


def set_led(pin, on):
    GPIO.output(pin, GPIO.HIGH if on else GPIO.LOW)
