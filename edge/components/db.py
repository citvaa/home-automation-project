class DBController:
    def __init__(self, settings):
        self.settings = settings
        self.state = False

    def set_state(self, on: bool):
        self.state = bool(on)
        if self.settings and self.settings.get("simulated", False):
            try:
                from edge.sim.db import BuzzerSimulator

                BuzzerSimulator.set_state(self.state)
            except Exception:
                pass
        else:
            try:
                from edge.hw.db import set_buzzer

                set_buzzer(self.settings["pin"], self.state)
            except Exception:
                pass

        from common.logging import get_logger
        get_logger(__name__).info("[DB] Buzzer set to %s", 'ON' if self.state else 'OFF')
