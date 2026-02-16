class BuzzerSimulator:
    state = False

    @classmethod
    def set_state(cls, on):
        cls.state = bool(on)
        from common.logging import get_logger
        get_logger(__name__).info("[SIM] Buzzer -> %s", 'ON' if on else 'OFF')
