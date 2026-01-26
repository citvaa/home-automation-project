import json
import queue
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, cast

try:
    import paho.mqtt.client as mqtt
except Exception:
    mqtt = None

try:
    from influxdb_client.client.influxdb_client import InfluxDBClient
    from influxdb_client.client.write.point import Point
    from influxdb_client.domain.write_precision import WritePrecision
except Exception:
    InfluxDBClient = None
    Point = None
    WritePrecision = None

from app.config.settings import Config, load_config


class MqttInfluxService:
    """Service that subscribes to MQTT topics and writes incoming messages to InfluxDB.

    - MQTT callbacks put messages into internal queue
    - A writer worker batches messages and writes them to InfluxDB
    - Also exposes a .publish_actuator(...) helper to publish actuator messages
    """

    def __init__(self, cfg: Config, mqtt_client: Optional[Any] = None, influx_client: Optional[Any] = None):
        self.cfg = cfg
        self._mqtt_client = mqtt_client
        self._internal_mqtt = mqtt_client is None
        self._influx_client = influx_client
        self._internal_influx = influx_client is None

        # queue stores tuples of (topic, payload)
        self._queue: "queue.Queue[Tuple[str, Any]]" = queue.Queue()
        self._stop_event = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None
        # write API (set in start)
        self._write_api: Any = None

        # config for writer batching
        self._batch_size = cfg.batch.batch_size
        self._max_interval = cfg.batch.max_interval_seconds

    def start(self):
        # setup influx client
        if self._internal_influx:
            if InfluxDBClient is None:
                raise RuntimeError("influxdb-client not installed")
            self._influx_client = InfluxDBClient(url=self.cfg.server.influx.url, token=self.cfg.server.influx.token, org=self.cfg.server.influx.org)
        # At this point _influx_client should be set (either provided or created). Guard and cast for type-checkers.
        if self._influx_client is None:
            raise RuntimeError("InfluxDB client instance not available")
        # cast for static checkers that may not infer non-None above
        self._write_api = cast(Any, self._influx_client).write_api()

        # setup mqtt client
        if self._internal_mqtt:
            if mqtt is None:
                raise RuntimeError("paho-mqtt not installed")
            self._mqtt_client = mqtt.Client()
            if self.cfg.mqtt.username:
                self._mqtt_client.username_pw_set(self.cfg.mqtt.username, self.cfg.mqtt.password)

            self._mqtt_client.on_connect = self._on_connect
            self._mqtt_client.on_message = self._on_message

            try:
                self._mqtt_client.connect(self.cfg.mqtt.broker, self.cfg.mqtt.port)
                self._mqtt_client.loop_start()
            except Exception as e:
                print(f"[MqttInfluxService] WARNING: MQTT connect failed: {e}")
        else:
            # user provided client must have on_message attribute set or we set ours
            if self._mqtt_client is not None:
                try:
                    self._mqtt_client.on_message = self._on_message
                except Exception:
                    pass

        # start writer thread
        self._worker_thread = threading.Thread(target=self._writer_loop, daemon=True)
        self._worker_thread.start()
        print("[MqttInfluxService] started")

    def stop(self):
        self._stop_event.set()
        if self._worker_thread:
            self._worker_thread.join(timeout=5)
        if self._internal_mqtt and self._mqtt_client is not None:
            try:
                self._mqtt_client.loop_stop()
                self._mqtt_client.disconnect()
            except Exception:
                pass
        if self._internal_influx and self._influx_client is not None:
            try:
                self._influx_client.close()
            except Exception:
                pass
        print("[MqttInfluxService] stopped")

    # MQTT callbacks
    def _on_connect(self, client, userdata, flags, rc):
        print(f"[MqttInfluxService] mqtt connected with rc={rc}")
        for topic in self.cfg.server.subscribe_topics:
            client.subscribe(topic)
            print(f"[MqttInfluxService] subscribed to {topic}")

    def _on_message(self, client, userdata, msg):
        try:
            payload = msg.payload.decode("utf-8")
            data = json.loads(payload)
        except Exception as e:
            print(f"[MqttInfluxService] Failed to decode message on {getattr(msg, 'topic', '<unknown>')}: {e}")
            return
        # push tuple(topic, data)
        # type: ignore[call-arg] - queue typed to hold tuples
        self._queue.put((msg.topic, data))

    # Writer thread
    def _writer_loop(self):
        buffer: List[Dict] = []
        last_flush = time.time()
        while not self._stop_event.is_set():
            try:
                topic, data = self._queue.get(timeout=0.5)
                # Expand if envelope with 'readings'
                if isinstance(data, dict) and "readings" in data and isinstance(data["readings"], list):
                    for r in data["readings"]:
                        entry = {
                            "topic": topic,
                            "payload": r,
                        }
                        buffer.append(entry)
                else:
                    buffer.append({"topic": topic, "payload": data})

                if len(buffer) >= self._batch_size:
                    self._flush_buffer(buffer)
                    buffer = []
                    last_flush = time.time()
            except queue.Empty:
                now = time.time()
                if buffer and (now - last_flush) >= self._max_interval:
                    self._flush_buffer(buffer)
                    buffer = []
                    last_flush = now
                continue
        # flush remaining
        if buffer:
            self._flush_buffer(buffer)

    def _flush_buffer(self, buffer: List[Dict]):
        points = []
        for e in buffer:
            topic = e["topic"]
            payload = e["payload"]
            # payload expected to have sensor fields or be a single reading
            try:
                sensor_type = payload.get("sensor_type") if isinstance(payload, dict) else None
                device_id = payload.get("device_id") if isinstance(payload, dict) else None
                device_name = payload.get("device_name") if isinstance(payload, dict) else None
                simulated = payload.get("simulated") if isinstance(payload, dict) else None
                value = payload.get("value") if isinstance(payload, dict) else None
                unit = payload.get("unit") if isinstance(payload, dict) else None
                timestamp = payload.get("timestamp") if isinstance(payload, dict) else None

                # construct Point
                if Point is None:
                    # fallback: store as raw JSON string
                    lines = {
                        "measurement": sensor_type or "sensors",
                        "tags": {"device_id": device_id or "unknown", "simulated": str(simulated)},
                        "fields": {"payload": json.dumps(payload)},
                    }
                    points.append(lines)
                else:
                    m = sensor_type or "sensors"
                    p = Point(m)
                    # tags
                    if device_id:
                        p.tag("device_id", device_id)
                    if device_name:
                        p.tag("device_name", device_name)
                    if sensor_type:
                        p.tag("sensor_type", sensor_type)
                    if simulated is not None:
                        p.tag("simulated", str(simulated))
                    if unit:
                        p.tag("unit", unit)
                    # fields
                    if isinstance(value, (int, float)):
                        p.field("value", float(value))
                    elif isinstance(value, bool):
                        p.field("value", int(value))
                    else:
                        # store as string
                        p.field("value_str", str(value))
                    # timestamp
                    if timestamp:
                        # try converting ISO timestamp to datetime and set time if possible
                        try:
                            if isinstance(timestamp, str):
                                # handle trailing Z
                                ts = timestamp.replace("Z", "+00:00")
                                dt = datetime.fromisoformat(ts)
                            elif isinstance(timestamp, (int, float)):
                                # assume epoch seconds
                                dt = datetime.fromtimestamp(float(timestamp), timezone.utc)
                            else:
                                dt = None

                            if dt is not None and WritePrecision is not None and hasattr(WritePrecision, "NS"):
                                p.time(dt, WritePrecision.NS)
                        except Exception:
                            # ignore timestamp parsing errors
                            pass
                    points.append(p)
            except Exception as ex:
                print(f"[MqttInfluxService] error preparing point: {ex}")
                continue

        # write to influx
        try:
            # Defensive: _write_api may be any object from different client versions
            if self._write_api is None:
                raise RuntimeError("InfluxDB write API not initialized")

            if hasattr(self._write_api, "write"):
                # influxdb-client modern API
                # write expects bucket/org/record
                self._write_api.write(bucket=self.cfg.server.influx.bucket, org=self.cfg.server.influx.org, record=points)
            elif hasattr(self._write_api, "write_points"):
                # fallback for other clients
                self._write_api.write_points(points)
            else:
                # last resort: try calling write directly
                try:
                    self._write_api(points)
                except Exception:
                    raise RuntimeError("No suitable write method found on InfluxDB client")

            print(f"[MqttInfluxService] wrote {len(points)} points to InfluxDB")
        except Exception as e:
            print(f"[MqttInfluxService] write error: {e}")

    # Helper to publish actuator messages
    def publish_actuator(self, actuator_type: str, payload: Dict[str, Any], qos: Optional[int] = None):
        topic = self.cfg.get_actuator_topic(actuator_type)
        if qos is None:
            qos = self.cfg.mqtt.qos
        # Ensure qos is an int (static checkers) and valid
        qos = int(qos)
        data = json.dumps(payload)
        if self._mqtt_client is None:
            print(f"[MqttInfluxService] No MQTT client; would publish to {topic}: {data}")
        else:
            self._mqtt_client.publish(topic, data, qos=qos)
            print(f"[MqttInfluxService] published actuator message to {topic}: {data}")


if __name__ == "__main__":
    cfg = load_config()
    svc = MqttInfluxService(cfg)
    try:
        svc.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        svc.stop()
