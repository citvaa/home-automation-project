import os
import sys
import time
import json
import types

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.config.settings import load_config
from app.server.mqtt_influx import MqttInfluxService


class FakeMqttClient:
    def __init__(self):
        self.subscribed = []
        self.published = []
        self.on_message = None

    def connect(self, host, port):
        return True

    def loop_start(self):
        pass

    def loop_stop(self):
        pass

    def disconnect(self):
        pass

    def subscribe(self, topic):
        self.subscribed.append(topic)

    def publish(self, topic, payload, qos=0):
        self.published.append((topic, payload, qos))


class FakeWriteApi:
    def __init__(self):
        self.written = []

    def write(self, bucket, org, record):
        self.written.append((bucket, org, record))


class FakeInfluxClient:
    def __init__(self):
        self._write_api = FakeWriteApi()

    def write_api(self):
        return self._write_api

    def close(self):
        pass


def test_service_write():
    cfg = load_config()
    fake_mqtt = FakeMqttClient()
    fake_influx = FakeInfluxClient()

    svc = MqttInfluxService(cfg, mqtt_client=fake_mqtt, influx_client=fake_influx)
    svc.start()

    # simulate on_connect
    svc._on_connect(fake_mqtt, None, None, 0)
    assert cfg.server.subscribe_topics[0] in fake_mqtt.subscribed

    # simulate incoming single reading
    msg = types.SimpleNamespace(topic=cfg.server.subscribe_topics[0], payload=json.dumps({"sensor_type": "DS1", "value": 12.3, "unit": "C", "device_id": cfg.device.id, "simulated": False}).encode('utf-8'))

    svc._on_message(fake_mqtt, None, msg)

    # writer needs a brief moment to flush
    time.sleep(0.2)

    # stop and flush
    svc.stop()

    assert len(fake_influx._write_api.written) >= 1


def test_publish_actuator_queued():
    cfg = load_config()
    fake_mqtt = FakeMqttClient()
    fake_influx = FakeInfluxClient()

    svc = MqttInfluxService(cfg, mqtt_client=fake_mqtt, influx_client=fake_influx)
    svc.start()

    # queue an actuator message and let publisher thread deliver it
    payload = {"state": "on", "device_id": cfg.device.id}
    ok = svc.publish_actuator('led', payload, device_id=cfg.device.id)
    assert ok is True

    # give publisher a moment
    time.sleep(0.2)

    svc.stop()

    assert len(fake_mqtt.published) >= 1
    topic, payload_str, qos = fake_mqtt.published[0]
    assert 'actuators' in topic
    assert 'state' in payload_str


def test_normalize_value_suffix_fallback():
    cfg = load_config()
    fake_mqtt = FakeMqttClient()
    fake_influx = FakeInfluxClient()

    # Force fallback behavior (Point = None) to inspect dict written
    import app.server.mqtt_influx as msvc
    orig_point = msvc.Point
    msvc.Point = None

    svc = MqttInfluxService(cfg, mqtt_client=fake_mqtt, influx_client=fake_influx)
    svc.start()

    # simulate incoming single reading with suffix value
    msg = types.SimpleNamespace(topic=cfg.server.subscribe_topics[0], payload=json.dumps({
        "sensor_type": "DS1", "value": "500m", "unit": None, "device_id": cfg.device.id, "simulated": False
    }).encode('utf-8'))

    svc._on_message(fake_mqtt, None, msg)
    time.sleep(0.2)
    svc.stop()

    # restore
    msvc.Point = orig_point

    assert len(fake_influx._write_api.written) >= 1
    # written is a tuple (bucket, org, record)
    bucket, org, record = fake_influx._write_api.written[0]
    assert isinstance(record, list)
    rec = record[0]
    # tags should include unit 'm'
    assert rec['tags'].get('unit') == 'm'
    # fields should include numeric value 500.0
    assert rec['fields'].get('value') == 500.0



if __name__ == "__main__":
    test_service_write()
    test_publish_actuator_queued()
    print("OK")
