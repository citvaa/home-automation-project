class DLController:
    def __init__(self, settings):
        self.settings = settings
        self.state = False

    def set_state(self, on: bool):
        self.state = bool(on)
        if self.settings and self.settings.get("simulated", False):
            try:
                from edge.sim.dl import LedSimulator

                LedSimulator.set_state(self.state)
            except Exception:
                pass
        else:
            try:
                from edge.hw.dl import set_led

                set_led(self.settings["pin"], self.state)
            except Exception:
                pass

        from common.logging import get_logger
        get_logger(__name__).info("[DL] LED set to %s", 'ON' if self.state else 'OFF')
