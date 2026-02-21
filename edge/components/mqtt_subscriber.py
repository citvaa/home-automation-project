import json
from typing import Any, Optional

try:
    import paho.mqtt.client as mqtt
except Exception:
    mqtt = None

from common.logging import get_logger
from config.settings import Config


class ActuatorSubscriber:
    """Subscribe to actuator MQTT topics and drive local controllers."""

    def __init__(self, cfg: Config, led_ctrl=None, buzzer_ctrl=None):
        self.cfg = cfg
        self.led_ctrl = led_ctrl
        self.buzzer_ctrl = buzzer_ctrl
        self._client: Optional[Any] = None

    def start(self):
        if mqtt is None:
            raise RuntimeError("paho-mqtt not installed")
        self._client = mqtt.Client()
        if self.cfg.mqtt.username:
            self._client.username_pw_set(self.cfg.mqtt.username, self.cfg.mqtt.password)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.connect(self.cfg.mqtt.broker, self.cfg.mqtt.port)
        self._client.loop_start()
        get_logger(__name__).info("ActuatorSubscriber started")

    def stop(self):
        if self._client is None:
            return
        try:
            self._client.loop_stop()
            self._client.disconnect()
        except Exception:
            pass
        get_logger(__name__).info("ActuatorSubscriber stopped")

    def _on_connect(self, client, userdata, flags, rc):
        topic = self.cfg.mqtt.actuator_topic_pattern.format(device_id=self.cfg.device.id, actuator_type="#")
        client.subscribe(topic)
        get_logger(__name__).info("Subscribed to %s", topic)

    def _on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except Exception:
            return

        if isinstance(payload, dict) and payload.get("source") == "device":
            return

        topic_parts = (msg.topic or "").split("/")
        actuator_type = topic_parts[-1] if topic_parts else ""
        state = self._coerce_state(payload)
        if state is None:
            return

        if actuator_type.lower() in ("led", "dl"):
            if self.led_ctrl:
                self.led_ctrl.set_state(state)
        elif actuator_type.lower() in ("buzzer", "db"):
            if self.buzzer_ctrl:
                self.buzzer_ctrl.set_state(state)

    def _coerce_state(self, payload):
        if not isinstance(payload, dict):
            return None
        if "state" in payload:
            val = str(payload.get("state")).lower()
            if val in ("on", "true", "1"):
                return True
            if val in ("off", "false", "0"):
                return False
        if "value" in payload:
            val = payload.get("value")
            if isinstance(val, bool):
                return val
            if isinstance(val, (int, float)):
                return bool(int(val))
            if isinstance(val, str):
                return val.lower() in ("true", "on", "1")
        return None
