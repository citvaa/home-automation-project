import threading
import json
import os
import urllib.request
import threading
from datetime import datetime, timezone
from common.logging import get_logger as _get_logger
from config.settings import load_config
from edge.sim.dl import LedSimulator

class DLController:
    def __init__(self, settings):
        self.settings = settings
        self.state = False

    def set_state(self, on: bool):
        self.state = bool(on)
        if self.settings and self.settings.get("simulated", False):
            try:
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

        # Non-blocking: publish actuator event to server so the rest of the system
        # (MQTT -> Influx -> Grafana) is notified of actuator changes.
        def _publish_actuator():
            try:
                cfg = load_config()
                payload = {
                    "sensor_type": "DL",
                    "value": 1 if self.state else 0,
                    "unit": None,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "simulated": self.settings.get("simulated", False) if self.settings else False,
                    "device_id": cfg.device.id,
                    "device_name": cfg.device.name,
                }
                # Allow overriding server URL via env var (useful in Docker)
                url = os.environ.get("SERVER_URL")
                if not url:
                    # If server is in docker compose use service name 'server', else localhost
                    host = os.environ.get("SERVER_HOST") or ("server" if os.environ.get("IN_DOCKER") else "localhost")
                    port = cfg.server.listen_port
                    url = f"http://{host}:{port}"

                req = urllib.request.Request(f"{url}/actuator/{cfg.device.id}/led", data=json.dumps(payload).encode("utf-8"), headers={"Content-Type": "application/json"})
                # short timeout and ignore response; run in background thread
                try:
                    with urllib.request.urlopen(req, timeout=2) as resp:
                        _get_logger(__name__).debug("actuator publish response: %s", resp.status)
                except Exception as e:
                    _get_logger(__name__).warning("actuator publish failed: %s", e)
            except Exception:
                # swallow all errors to avoid affecting main flow
                from common.logging import get_logger as _g
                _g(__name__).debug("failed publishing actuator event", exc_info=True)

        try:
            t = threading.Thread(target=_publish_actuator, daemon=True)
            t.start()
        except Exception:
            # Do not let background publish interfere with LED control
            pass
