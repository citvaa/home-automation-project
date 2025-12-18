import RPi.GPIO as GPIO


def setup_buzzer(pin):
    GPIO.setup(pin, GPIO.OUT)
    GPIO.output(pin, GPIO.LOW)


def set_buzzer(pin, on):
    GPIO.output(pin, GPIO.HIGH if on else GPIO.LOW)
