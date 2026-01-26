import time
import json
import random
import os

import paho.mqtt.client as mqtt

MQTT_BROKER = os.environ.get("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))
DEVICE_ID = os.environ.get("DEVICE_ID", "PI1")

client = mqtt.Client()

# Retry connection until broker is ready
connected = False
while not connected:
    try:
        client.connect(MQTT_BROKER, MQTT_PORT)
        connected = True
    except Exception as e:
        print(f"[simulator] MQTT connect failed ({e}); retrying in 2s...")
        time.sleep(2)

client.loop_start()

try:
    while True:
        # Publish DS1 boolean
        payload = {
            "device_id": DEVICE_ID,
            "sensor_type": "DS1",
            "value": random.choice([True, False]),
            "simulated": True,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
        client.publish(f"sensors/{DEVICE_ID}/DS1", json.dumps(payload), qos=1)

        # Publish DUS1 distance
        payload = {
            "device_id": DEVICE_ID,
            "sensor_type": "DUS1",
            "value": round(random.uniform(30.0, 120.0), 2),
            "unit": "cm",
            "simulated": True,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
        client.publish(f"sensors/{DEVICE_ID}/DUS1", json.dumps(payload), qos=1)

        time.sleep(5)
except KeyboardInterrupt:
    client.loop_stop()
    client.disconnect()
