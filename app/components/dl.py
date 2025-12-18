class DLController:
    def __init__(self, settings):
        self.settings = settings or {}
        self.simulated = self.settings.get("simulated", True)
        self.pin = self.settings.get("pin")
        self.available = self.pin is not None

        if not self.available:
            print("DL pin not configured; LED controller unavailable.")
            return

        if self.simulated:
            from app.sim.dl import LedSimulator

            self.driver = LedSimulator()
        else:
            try:
                from app.hw.dl import setup_led, set_led
            except ImportError:
                print("RPi.GPIO not available; LED controller disabled.")
                self.available = False
                return

            setup_led(self.pin)
            self.driver = lambda on: set_led(self.pin, on)

    def set_state(self, on):
        if not self.available:
            print("LED controller not available.")
            return

        if self.simulated:
            self.driver.set_state(on)
        else:
            self.driver(on)
