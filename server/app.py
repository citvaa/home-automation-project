from flask import Flask, jsonify, request
import threading
from typing import Any, Dict, cast

from config.settings import load_config
from server.mqtt_influx import MqttInfluxService

app = Flask(__name__)
cfg = load_config()

# Create a single service instance
_svc = MqttInfluxService(cfg)


@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/status/<device_id>", methods=["GET"])
def status(device_id):
    try:
        return jsonify(_svc.get_current_state(device_id)), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/actuator/<device_id>/<actuator_type>", methods=["POST"])
def actuator(device_id, actuator_type):
    payload = cast(Dict[str, Any], request.get_json() or {})

    # attach device info
    payload.setdefault("device_id", device_id)
    # attach sensor_type + value for actuator points (so they are written to InfluxDB)
    if "sensor_type" not in payload:
        actuator_map = {"led": "DL", "buzzer": "DB"}
        payload["sensor_type"] = actuator_map.get(actuator_type) or actuator_type
    if "value" not in payload and payload.get("state") is not None:
        state_val = str(payload.get("state")).lower()
        if state_val in ("on", "true", "1"):
            payload["value"] = True
        elif state_val in ("off", "false", "0"):
            payload["value"] = False
    # Publish using service
    try:
        _svc.publish_actuator(actuator_type, payload, device_id=device_id)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"status": "published", "method": request.method}), 200


@app.route("/security/<device_id>", methods=["GET"])
def security_state(device_id):
    try:
        return jsonify(_svc.get_security_snapshot(device_id)), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/security/<device_id>/arm", methods=["POST"])
def security_arm(device_id):
    payload = cast(Dict[str, Any], request.get_json() or {})
    pin = str(payload.get("pin", ""))
    res = _svc.arm_system(device_id, pin)
    return jsonify(res), 200 if res.get("ok") else 400


@app.route("/security/<device_id>/disarm", methods=["POST"])
def security_disarm(device_id):
    payload = cast(Dict[str, Any], request.get_json() or {})
    pin = str(payload.get("pin", ""))
    res = _svc.disarm_system(device_id, pin)
    return jsonify(res), 200 if res.get("ok") else 400


def start_service():
    _svc.start()


def stop_service():
    _svc.stop()


if __name__ == "__main__":
    # Start service in background and run flask
    t = threading.Thread(target=start_service, daemon=True)
    t.start()
    app.run(host=cfg.server.listen_host, port=cfg.server.listen_port)
