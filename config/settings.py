import json
import os
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Union

# Accept both str and Path for file paths
PathLike = Union[str, Path]


def load_settings(file_path: Optional[PathLike] = None) -> Dict[str, Any]:
    """Load raw settings JSON as a dictionary.

    `file_path` can be a `str` or `pathlib.Path`. If not provided, the default
    is `config/settings.json` next to this module.
    """
    if file_path is None:
        file_path = Path(__file__).resolve().parent / "settings.json"
    else:
        # coerce str -> Path, if a Path was provided leave as-is
        if not isinstance(file_path, Path):
            file_path = Path(file_path)

    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


@dataclass
class DeviceConfig:
    id: str = "PI1"
    name: str = "pi1"
    location: Optional[str] = None


@dataclass
class MqttConfig:
    broker: str = "localhost"
    port: int = 1883
    username: str = ""
    password: str = ""
    qos: int = 1
    base_topic: str = "home"
    sensor_topic_pattern: str = "sensors/{device_id}/{sensor_type}"
    actuator_topic_pattern: str = "actuators/{device_id}/{actuator_type}"
    batch_topic_pattern: str = "sensors/{device_id}/batch"
    per_sensor_topics: Dict[str, str] = field(default_factory=dict)

# ... later in Config class, add get_batch_topic method (replace snippet after existing get_actuator_topic)


@dataclass
class BatchConfig:
    enabled: bool = True
    mode: str = "global"  # 'global' or 'per_sensor'
    batch_size: int = 10
    max_interval_seconds: int = 5


@dataclass
class InfluxConfig:
    url: str = "http://localhost:8086"
    token: str = ""
    org: str = ""
    bucket: str = "sensors"
    actuator_bucket: str = "actuators"


@dataclass
class ServerConfig:
    influx: InfluxConfig = field(default_factory=InfluxConfig)
    subscribe_topics: List[str] = field(default_factory=lambda: ["sensors/#", "actuators/#"])
    listen_host: str = "0.0.0.0"
    listen_port: int = 5000


@dataclass
class Config:
    device: DeviceConfig = field(default_factory=DeviceConfig)
    mqtt: MqttConfig = field(default_factory=MqttConfig)
    batch: BatchConfig = field(default_factory=BatchConfig)
    server: ServerConfig = field(default_factory=ServerConfig)

    # Raw sensor-specific configuration entries (kept as-is)
    sensors: Dict[str, Any] = field(default_factory=dict)

    def get_sensor_topic(self, sensor_type: str) -> str:
        """Return the resolved MQTT topic for a sensor type using per-sensor override if available."""
        if sensor_type in self.mqtt.per_sensor_topics:
            return self.mqtt.per_sensor_topics[sensor_type].format(device_id=self.device.id)
        return self.mqtt.sensor_topic_pattern.format(device_id=self.device.id, sensor_type=sensor_type)

    def get_actuator_topic(self, actuator_type: str) -> str:
        return self.mqtt.actuator_topic_pattern.format(device_id=self.device.id, actuator_type=actuator_type)

    def get_batch_topic(self, sensor_type: Optional[str] = None) -> str:
        """Return the topic to publish batches to.

        - If batch.mode == 'global' -> uses `mqtt.batch_topic_pattern`
        - If batch.mode == 'per_sensor' -> requires `sensor_type` and returns sensor topic for that type
        """
        if self.batch.mode == "global":
            return self.mqtt.batch_topic_pattern.format(device_id=self.device.id)
        # per_sensor
        if sensor_type is None:
            raise ValueError("sensor_type must be provided when batch.mode == 'per_sensor'")
        return self.get_sensor_topic(sensor_type)


def _merge_dict_to_dataclass(dc, data: Dict[str, Any]):
    """Helper: merge a dict into a dataclass instance (recursively for nested dataclasses)."""
    for k, v in data.items():
        if not hasattr(dc, k):
            continue
        attr = getattr(dc, k)
        # If target field is a dataclass, recurse
        if hasattr(attr, "__dataclass_fields__") and isinstance(v, dict):
            _merge_dict_to_dataclass(attr, v)
        else:
            setattr(dc, k, v)


def load_config(file_path: Optional[PathLike] = None) -> Config:
    """Load settings JSON and return a typed `Config` dataclass with defaults and validation."""
    raw = load_settings(file_path)

    cfg = Config()

    # Merge top-level structured sections if present
    if "device" in raw and isinstance(raw["device"], dict):
        _merge_dict_to_dataclass(cfg.device, raw["device"])

    if "mqtt" in raw and isinstance(raw["mqtt"], dict):
        _merge_dict_to_dataclass(cfg.mqtt, raw["mqtt"])

    if "batch" in raw and isinstance(raw["batch"], dict):
        _merge_dict_to_dataclass(cfg.batch, raw["batch"])

    if "server" in raw and isinstance(raw["server"], dict):
        # handle nested influx
        server_raw = raw["server"]
        if "influx" in server_raw and isinstance(server_raw["influx"], dict):
            _merge_dict_to_dataclass(cfg.server.influx, server_raw["influx"])
        if "subscribe_topics" in server_raw:
            cfg.server.subscribe_topics = server_raw.get("subscribe_topics", cfg.server.subscribe_topics)
        cfg.server.listen_host = server_raw.get("listen_host", cfg.server.listen_host)
        cfg.server.listen_port = server_raw.get("listen_port", cfg.server.listen_port)

    # Environment overrides (useful for Docker/host differences)
    env_broker = os.environ.get("MQTT_BROKER")
    if env_broker:
        cfg.mqtt.broker = env_broker
    env_port = os.environ.get("MQTT_PORT")
    if env_port:
        try:
            cfg.mqtt.port = int(env_port)
        except ValueError:
            pass

    # Merge per-sensor keys (anything not in known sections)
    known = {"device", "mqtt", "batch", "server"}
    sensors = {k: v for k, v in raw.items() if k not in known}
    cfg.sensors.update(sensors)

    # Basic validation
    if cfg.batch.mode not in ("global", "per_sensor"):
        raise ValueError("batch.mode must be 'global' or 'per_sensor'")
    if cfg.mqtt.qos not in (0, 1, 2):
        raise ValueError("mqtt.qos must be 0, 1 or 2")
    if cfg.batch.batch_size <= 0:
        raise ValueError("batch.batch_size must be > 0")

    return cfg


# Convenience: allow importing Config and load_config directly from module
__all__ = ["DeviceConfig", "MqttConfig", "BatchConfig", "InfluxConfig", "ServerConfig", "Config", "load_config"]
