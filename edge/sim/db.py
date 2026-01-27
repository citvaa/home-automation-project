class BuzzerSimulator:
    state = False

    @classmethod
    def set_state(cls, on):
        cls.state = bool(on)
        print(f"[SIM] Buzzer -> {'ON' if on else 'OFF'}")
