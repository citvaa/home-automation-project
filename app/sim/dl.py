class LedSimulator:
    def __init__(self):
        self.state = False

    def set_state(self, on):
        self.state = on
        print(f"[SIM] LED -> {'ON' if on else 'OFF'}")
