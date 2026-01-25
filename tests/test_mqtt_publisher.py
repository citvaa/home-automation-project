import os
import sys
import time
import json

# Allow running this test directly by adding project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.config.settings import load_config
from app.components.mqtt_publisher import BatchPublisher


class FakeClient:
    def __init__(self):
        self.published = []

    def publish(self, topic, payload, qos=0):
        self.published.append((topic, payload, qos))


def wait_for_publishes(fake, min_count=1, timeout=3.0):
    start = time.time()
    while time.time() - start < timeout:
        if len(fake.published) >= min_count:
            return True
        time.sleep(0.05)
    return False


def test_global_batch_by_size():
    cfg = load_config()
    cfg.batch.batch_size = 3
    cfg.batch.max_interval_seconds = 5
    cfg.batch.mode = "global"

    fake = FakeClient()
    pub = BatchPublisher(cfg, mqtt_client=fake)
    pub.start()

    for i in range(5):
        pub.enqueue_reading({"sensor_type": "DS1", "value": i, "unit": "C", "simulated": True})

    # batch_size=3 -> should publish at least 1 batch quickly
    assert wait_for_publishes(fake, min_count=1, timeout=2.0)

    pub.stop()
    # After stop, remaining should be flushed
    assert len(fake.published) >= 2


def test_per_sensor_batch_by_interval():
    cfg = load_config()
    cfg.batch.batch_size = 100
    cfg.batch.max_interval_seconds = 1
    cfg.batch.mode = "per_sensor"

    fake = FakeClient()
    pub = BatchPublisher(cfg, mqtt_client=fake)
    pub.start()

    pub.enqueue_reading({"sensor_type": "DS1", "value": 1, "unit": "C", "simulated": False})

    # Wait for interval-based flush
    assert wait_for_publishes(fake, min_count=1, timeout=3.0)

    pub.stop()
    assert len(fake.published) >= 1


if __name__ == "__main__":
    test_global_batch_by_size()
    test_per_sensor_batch_by_interval()
    print("OK")
