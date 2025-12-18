class DBController:
    def __init__(self, settings):
        self.settings = settings or {}
        self.simulated = self.settings.get("simulated", True)
        self.pin = self.settings.get("pin")
        self.available = self.pin is not None

        if not self.available:
            print("DB pin not configured; buzzer controller unavailable.")
            return

        if self.simulated:
            from app.sim.db import BuzzerSimulator

            self.driver = BuzzerSimulator()
        else:
            try:
                from app.hw.db import setup_buzzer, set_buzzer
            except ImportError:
                print("RPi.GPIO not available; buzzer controller disabled.")
                self.available = False
                return

            setup_buzzer(self.pin)
            self.driver = lambda on: set_buzzer(self.pin, on)

    def set_state(self, on):
        if not self.available:
            print("Buzzer controller not available.")
            return

        if self.simulated:
            self.driver.set_state(on)
        else:
            self.driver(on)
