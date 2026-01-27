class LedSimulator:
    state = False

    @classmethod
    def set_state(cls, on):
        cls.state = bool(on)
        print(f"[SIM] LED -> {'ON' if on else 'OFF'}")
