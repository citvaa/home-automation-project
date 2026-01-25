from flask import Flask, jsonify, request
import threading

from app.config.settings import load_config
from app.server.mqtt_influx import MqttInfluxService

app = Flask(__name__)
cfg = load_config()

# Create a single service instance
_svc = MqttInfluxService(cfg)

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200

@app.route("/actuator/<device_id>/<actuator_type>", methods=["POST"])
def actuator(device_id, actuator_type):
    payload = request.get_json() or {}
    # attach device info
    payload.setdefault("device_id", device_id)
    payload.setdefault("timestamp", request.json.get("timestamp") if request.json else None)
    # Publish using service
    try:
        _svc.publish_actuator(actuator_type, payload)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"status": "published"}), 200


def start_service():
    _svc.start()


def stop_service():
    _svc.stop()


if __name__ == "__main__":
    # Start service in background and run flask
    t = threading.Thread(target=start_service, daemon=True)
    t.start()
    app.run(host=cfg.server.listen_host, port=cfg.server.listen_port)
