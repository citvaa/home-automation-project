import json
import queue
import threading
import time
import re
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

from config.settings import Config, load_config
from server.utils import normalize_value, parse_timestamp


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
        # actuator publish queue (topic, payload_json, qos)
        self._actuator_queue: "queue.Queue[Tuple[str, str, int]]" = queue.Queue(maxsize=1000)

        self._stop_event = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None
        # publisher and monitoring threads
        self._publisher_thread: Optional[threading.Thread] = None
        self._monitor_thread: Optional[threading.Thread] = None

        # write API (set in start)
        self._write_api: Any = None

        # config for writer batching
        self._batch_size = cfg.batch.batch_size
        self._max_interval = cfg.batch.max_interval_seconds

        # monitoring / thresholds
        self._monitor_interval = 10  # seconds between health logs
        self._max_queue_size_warn = 500
        self._publish_timeout_warning = 2.0  # seconds
        self._write_timeout_warning = 2.0  # seconds (log if write takes longer)    

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
        # ensure actuator bucket exists (best-effort)
        self._ensure_buckets()

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
        # start actuator publisher thread
        self._publisher_thread = threading.Thread(target=self._publisher_loop, daemon=True)
        self._publisher_thread.start()
        # start monitor thread
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        print("[MqttInfluxService] started")

    def stop(self):
        self._stop_event.set()
        if self._worker_thread:
            self._worker_thread.join(timeout=5)
        # stop publisher
        if self._publisher_thread:
            self._publisher_thread.join(timeout=5)
        # stop monitor
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)

        # drain actuator queue trying to publish remaining messages (best-effort)
        try:
            while not self._actuator_queue.empty():
                try:
                    topic, data, qos = self._actuator_queue.get_nowait()
                    if self._mqtt_client is not None:
                        try:
                            self._mqtt_client.publish(topic, data, qos=qos)
                        except Exception:
                            pass
                except queue.Empty:
                    break
        except Exception:
            pass

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
        # log received message for debugging
        try:
            print(f"[MqttInfluxService] received message on {msg.topic}: {data}")
        except Exception:
            pass
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
        # debug: show what's being flushed
        try:
            sample = [e["payload"] for e in buffer][:3]
            print(f"[MqttInfluxService] flushing {len(buffer)} messages; sample payloads: {sample}")
        except Exception:
            pass
        points_by_bucket: Dict[str, List[Any]] = {}
        for e in buffer:
            topic = e["topic"]
            payload = e["payload"]
            bucket = self._get_bucket_for_topic(topic)
            # payload expected to have sensor fields or be a single reading
            try:
                sensor_type = payload.get("sensor_type") if isinstance(payload, dict) else None
                device_id = payload.get("device_id") if isinstance(payload, dict) else None
                device_name = payload.get("device_name") if isinstance(payload, dict) else None
                simulated = payload.get("simulated") if isinstance(payload, dict) else None
                value = payload.get("value") if isinstance(payload, dict) else None
                unit = payload.get("unit") if isinstance(payload, dict) else None
                timestamp = payload.get("timestamp") if isinstance(payload, dict) else None

                # Normalize value using shared utility (coerce strings to bool/number and detect unit)
                try:
                    value, unit = normalize_value(value, unit)
                except Exception as ex:
                    # never allow normalization to raise
                    print(f"[MqttInfluxService] normalization error: {ex}")

                # construct Point
                if Point is None:
                    # fallback: construct structured dict with tags and normalized fields
                    fields: Dict[str, Any] = {}
                    # fields for common types
                    if isinstance(value, (int, float)):
                        fields["value"] = float(value)
                    elif isinstance(value, bool):
                        fields["value"] = int(value)
                    else:
                        fields["value_str"] = str(value)

                    tags = {"device_id": device_id or "unknown", "simulated": str(simulated)}
                    if unit:
                        tags["unit"] = str(unit)

                    lines = {
                        "measurement": sensor_type or "sensors",
                        "tags": tags,
                        "fields": fields,
                    }
                    points_by_bucket.setdefault(bucket, []).append(lines)
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
                    # fields (normalized)
                    if isinstance(value, (int, float)):
                        p.field("value", float(value))
                    elif isinstance(value, bool):
                        p.field("value", int(value))
                    else:
                        p.field("value_str", str(value))
                    # timestamp
                    if timestamp:
                        dt = parse_timestamp(timestamp)
                        if dt is not None and WritePrecision is not None and hasattr(WritePrecision, "NS"):
                            p.time(dt, WritePrecision.NS)
                    points_by_bucket.setdefault(bucket, []).append(p)
            except Exception as ex:
                print(f"[MqttInfluxService] error preparing point: {ex}")
                continue

        # write to influx
        try:
            # Defensive: _write_api may be any object from different client versions
            if self._write_api is None:
                raise RuntimeError("InfluxDB write API not initialized")
            for bucket, points in points_by_bucket.items():
                if not points:
                    continue
                # time the write and warn if it takes too long
                start_write = time.time()
                if hasattr(self._write_api, "write"):
                    # influxdb-client modern API
                    # write expects bucket/org/record
                    self._write_api.write(bucket=bucket, org=self.cfg.server.influx.org, record=points)
                elif hasattr(self._write_api, "write_points"):
                    # fallback for other clients
                    self._write_api.write_points(points)
                else:
                    # last resort: try calling write directly
                    try:
                        self._write_api(points)
                    except Exception:
                        raise RuntimeError("No suitable write method found on InfluxDB client")
                write_dur = time.time() - start_write
                if write_dur > self._write_timeout_warning:
                    print(f"[MqttInfluxService] WARNING: Influx write took {write_dur:.2f}s for {len(points)} points")
                print(f"[MqttInfluxService] wrote {len(points)} points to InfluxDB bucket='{bucket}' in {write_dur:.3f}s")
        except Exception as e:
            print(f"[MqttInfluxService] write error: {e}")

    def _get_bucket_for_topic(self, topic: str) -> str:
        if topic.startswith("actuators/"):
            return self.cfg.server.influx.actuator_bucket
        return self.cfg.server.influx.bucket

    def _ensure_buckets(self):
        try:
            if self._influx_client is None or not hasattr(self._influx_client, "buckets_api"):
                return
            api = self._influx_client.buckets_api()
            for name in {self.cfg.server.influx.bucket, self.cfg.server.influx.actuator_bucket}:
                if not name:
                    continue
                try:
                    if api.find_bucket_by_name(name) is None:
                        api.create_bucket(bucket_name=name, org=self.cfg.server.influx.org)
                        print(f"[MqttInfluxService] created bucket '{name}'")
                except Exception as ex:
                    print(f"[MqttInfluxService] WARNING: bucket ensure failed for '{name}': {ex}")
        except Exception as ex:
            print(f"[MqttInfluxService] WARNING: bucket ensure failed: {ex}")

    # Helper to publish actuator messages
    def publish_actuator(self, actuator_type: str, payload: Dict[str, Any], qos: Optional[int] = None, device_id: Optional[str] = None):
        # allow overriding device_id for topic construction (useful for per-request device ids)
        if device_id is None:
            topic = self.cfg.get_actuator_topic(actuator_type)
        else:
            topic = self.cfg.mqtt.actuator_topic_pattern.format(device_id=device_id, actuator_type=actuator_type)
        if qos is None:
            qos = self.cfg.mqtt.qos
        # Ensure qos is an int (static checkers) and valid
        qos = int(qos)
        data = json.dumps(payload)
        # Enqueue non-blocking to avoid blocking callers (e.g., Flask request thread)
        try:
            self._actuator_queue.put_nowait((topic, data, qos))
            print(f"[MqttInfluxService] enqueued actuator message to {topic}: {data}")
            return True
        except queue.Full:
            # drop and log
            print(f"[MqttInfluxService] WARNING: actuator queue full, dropping message to {topic}")
            return False

    def _publisher_loop(self):
        """Worker that pulls actuator messages off a queue and publishes them via MQTT.
        Keeps publishes off request threads to avoid blocking I/O in callbacks/handlers.
        """
        while not self._stop_event.is_set():
            try:
                topic, data, qos = self._actuator_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                start = time.time()
                if self._mqtt_client is None:
                    print(f"[MqttInfluxService] No MQTT client; actuator publish skipped for {topic}")
                else:
                    res = self._mqtt_client.publish(topic, data, qos=qos)
                    dur = time.time() - start
                    if dur > self._publish_timeout_warning:
                        print(f"[MqttInfluxService] WARNING: actuator publish to {topic} took {dur:.2f}s")
                    print(f"[MqttInfluxService] actuator publish result for {topic}: {res}")
            except Exception as e:
                print(f"[MqttInfluxService] actuator publish error for {topic}: {e}")

    def _monitor_loop(self):
        """Periodic health checks and logging for queues to detect backpressure/deadlocks."""
        while not self._stop_event.wait(self._monitor_interval):
            try:
                in_q = self._queue.qsize()
                act_q = self._actuator_queue.qsize()
                print(f"[MqttInfluxService] queue sizes - incoming:{in_q}, actuator:{act_q}")
                if in_q > self._max_queue_size_warn:
                    print(f"[MqttInfluxService] WARNING: incoming queue size {in_q} exceeds threshold {self._max_queue_size_warn}")
                if act_q > self._max_queue_size_warn:
                    print(f"[MqttInfluxService] WARNING: actuator queue size {act_q} exceeds threshold {self._max_queue_size_warn}")
            except Exception:
                pass


if __name__ == "__main__":
    cfg = load_config()
    svc = MqttInfluxService(cfg)
    try:
        svc.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        svc.stop()
