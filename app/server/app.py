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

@app.route("/actuator/<device_id>/<actuator_type>", methods=["GET", "POST"])
def actuator(device_id, actuator_type):
    # Support GET for simple button-driven requests: /actuator/<device>/<type>?state=on
    if request.method == "GET":
        state = request.args.get("state")
        if not state:
            return jsonify({"error": "query parameter 'state' is required for GET"}), 400
        payload = {"state": state}
    else:
        payload = request.get_json() or {}

    # attach device info
    payload.setdefault("device_id", device_id)
    # use get_json(silent=True) so GET requests without Content-Type don't cause a 415
    req_json = request.get_json(silent=True)
    if req_json and req_json.get("timestamp") is not None:
        payload.setdefault("timestamp", req_json.get("timestamp"))
    # Publish using service
    try:
        _svc.publish_actuator(actuator_type, payload, device_id=device_id)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"status": "published", "method": request.method}), 200


def start_service():
    _svc.start()


def stop_service():
    _svc.stop()


if __name__ == "__main__":
    # Start service in background and run flask
    t = threading.Thread(target=start_service, daemon=True)
    t.start()
    app.run(host=cfg.server.listen_host, port=cfg.server.listen_port)
