import json
import queue
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    import paho.mqtt.client as mqtt
except Exception:  # paho may not be installed in test envs
    mqtt = None

from app.config.settings import Config, load_config

FORMAT_VERSION = "1.0"


class BatchPublisher:
    """Collect sensor readings and publish them in batches via MQTT.

    - Thread-safe enqueue with `enqueue_reading(dict)`
    - Runs a daemon worker that flushes batches by size or interval
    - Supports `global` and `per_sensor` batch modes (configured via Config)
    """

    def __init__(self, cfg: Config, mqtt_client: Optional[Any] = None):
        self.cfg = cfg
        self._queue: "queue.Queue[Dict]" = queue.Queue()
        self._stop_event = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None
        self._mqtt_client = mqtt_client
        self._internal_client = mqtt_client is None

        # readability
        self._batch_size = cfg.batch.batch_size
        self._max_interval = cfg.batch.max_interval_seconds
        self._mode = cfg.batch.mode

    # Public API
    def start(self):
        # establish mqtt client if necessary
        if self._internal_client:
            if mqtt is None:
                raise RuntimeError("paho-mqtt is required to use BatchPublisher without an injected client")
            self._mqtt_client = mqtt.Client()
            if self.cfg.mqtt.username:
                self._mqtt_client.username_pw_set(self.cfg.mqtt.username, self.cfg.mqtt.password)
            # non-blocking network loop
            try:
                self._mqtt_client.connect(self.cfg.mqtt.broker, self.cfg.mqtt.port)
                self._mqtt_client.loop_start()
            except Exception as e:
                print(f"[BatchPublisher] WARNING: MQTT connect failed: {e}")

        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()
        print("[BatchPublisher] started")

    def stop(self):
        self._stop_event.set()
        if self._worker_thread:
            self._worker_thread.join(timeout=5)
        # flush remaining
        try:
            self._flush_all()
        finally:
            if self._internal_client and self._mqtt_client is not None:
                try:
                    self._mqtt_client.loop_stop()
                    self._mqtt_client.disconnect()
                except Exception:
                    pass
        print("[BatchPublisher] stopped")

    def enqueue_reading(self, reading: Dict[str, Any]):
        """Enqueue a single reading dict.

        Expected minimal keys: `sensor_type`, `value`, `timestamp` (ISO str) or will be added.
        Caller should include `simulated` flag.
        """
        # ensure timestamp
        if "timestamp" not in reading:
            reading["timestamp"] = datetime.now(timezone.utc).isoformat()
        # minimal normalization
        self._queue.put(reading)

    # Internal worker
    def _worker_loop(self):
        if self._mode == "per_sensor":
            self._per_sensor_worker()
        else:
            self._global_worker()

    def _global_worker(self):
        batch: List[Dict[str, Any]] = []
        last_flush = time.time()
        while not self._stop_event.is_set():
            try:
                item = self._queue.get(timeout=0.5)
                batch.append(item)
                if len(batch) >= self._batch_size:
                    self._publish_batch(batch)
                    batch = []
                    last_flush = time.time()
            except queue.Empty:
                # check timeout
                now = time.time()
                if batch and (now - last_flush) >= self._max_interval:
                    self._publish_batch(batch)
                    batch = []
                    last_flush = now
                continue
        # finish up
        if batch:
            self._publish_batch(batch)

    def _per_sensor_worker(self):
        batches: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        last_flush_map: Dict[str, float] = defaultdict(lambda: time.time())
        while not self._stop_event.is_set():
            try:
                item = self._queue.get(timeout=0.5)
                sensor_type = item.get("sensor_type")
                if not sensor_type:
                    # fallback to unknown
                    sensor_type = "unknown"
                    item["sensor_type"] = sensor_type
                batches[sensor_type].append(item)
                if len(batches[sensor_type]) >= self._batch_size:
                    self._publish_batch(batches[sensor_type], sensor_type=sensor_type)
                    batches[sensor_type] = []
                    last_flush_map[sensor_type] = time.time()
            except queue.Empty:
                now = time.time()
                # check for any batches exceeding interval
                for s_type, reading_list in list(batches.items()):
                    if reading_list and (now - last_flush_map[s_type]) >= self._max_interval:
                        self._publish_batch(reading_list, sensor_type=s_type)
                        batches[s_type] = []
                        last_flush_map[s_type] = now
                continue
        # flush remaining
        for s_type, reading_list in batches.items():
            if reading_list:
                self._publish_batch(reading_list, sensor_type=s_type)

    def _publish_batch(self, readings: List[Dict[str, Any]], sensor_type: Optional[str] = None):
        envelope = {
            "device_id": self.cfg.device.id,
            "device_name": self.cfg.device.name,
            "batch_timestamp": datetime.now(timezone.utc).isoformat(),
            "format_version": FORMAT_VERSION,
            "readings": readings,
        }
        try:
            if self._mode == "global":
                topic = self.cfg.get_batch_topic()
            else:
                topic = self.cfg.get_batch_topic(sensor_type=sensor_type)
            payload = json.dumps(envelope)
            # publish
            if self._mqtt_client is None:
                print(f"[BatchPublisher] No MQTT client; would publish to {topic}: {payload}")
            else:
                self._mqtt_client.publish(topic, payload, qos=self.cfg.mqtt.qos)
                print(f"[BatchPublisher] published {len(readings)} readings to {topic}")
        except Exception as e:
            print(f"[BatchPublisher] publish error: {e}")

    def _flush_all(self):
        # drain queue and publish in current mode
        try:
            while True:
                item = self._queue.get_nowait()
                self._enqueue_direct(item)
        except queue.Empty:
            pass

    def _enqueue_direct(self, item: Dict[str, Any]):
        # internal helper used during stop to ensure all messages sent
        if self._mode == "global":
            self._publish_batch([item])
        else:
            s_type = item.get("sensor_type") or "unknown"
            self._publish_batch([item], sensor_type=s_type)


# Simple demo when executed directly
if __name__ == "__main__":
    cfg = load_config()
    pub = BatchPublisher(cfg)
    try:
        pub.start()
        for i in range(25):
            pub.enqueue_reading({"sensor_type": "DS1", "value": i, "unit": "C", "simulated": True})
            time.sleep(0.2)
        time.sleep(6)
    finally:
        pub.stop()
