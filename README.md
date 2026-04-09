# Home Automation Project

An IoT system for home monitoring and control, built with MQTT messaging, InfluxDB storage, a Flask API, an Angular web app, and Grafana dashboards.

## Architecture

The project consists of the following parts:

- **edge/** - Simulation or real hardware integration for sensors/actuators (DS1, DPIR1, DUS1, DMS, WEBC, DL, DB), including MQTT publish/subscribe logic.
- **server/** - Flask API + service that consumes MQTT messages, writes data to InfluxDB, and manages security state (arming/disarming/alarm).
- **webapp/** - Angular client for monitoring device state and controlling the security mode.
- **docker-compose.yml** - Infrastructure and backend services (Mosquitto, InfluxDB, Grafana, app, device).

Data flow:
1. Edge publishes sensor data to MQTT.
2. Server consumes MQTT messages and stores them in InfluxDB.
3. Web app reads current state through the Flask API.
4. Actuator and security commands are sent through the API back to edge devices.

## Technologies

- Python 3.11
- Flask
- Eclipse Mosquitto (MQTT broker)
- InfluxDB 2.7
- Grafana
- Angular 21
- Docker / Docker Compose

## Running the project (recommended: Docker)

Prerequisites:
- Docker Desktop (with Docker Compose support)
- Node.js + npm (for the web app only)

From the project root, run:

```powershell
docker compose up -d --build mosquitto influxdb grafana app
docker compose run --rm --service-ports device
```

On Windows, you can also run:

```powershell
.\run-device-interactive.ps1
```

Services after startup:

- Flask API: `http://localhost:5000`
- Grafana: `http://localhost:3000` (admin / admin)
- InfluxDB: `http://localhost:8086`
- MQTT broker: `localhost:1883`

## Running the web application

The web app is not included as a Docker Compose service, so run it locally:

```powershell
cd webapp
npm install
npm run start
```

The UI is available at `http://localhost:4200`.

## API routes

Main server routes:

- `GET /health` - Health check
- `GET /status/<device_id>` - Current sensor/actuator state and security snapshot
- `POST /actuator/<device_id>/<actuator_type>` - Send actuator command (e.g. `led`, `buzzer`)
- `GET /security/<device_id>` - Current security state
- `POST /security/<device_id>/arm` - Arm system (`{"pin":"1234"}`)
- `POST /security/<device_id>/disarm` - Disarm system (`{"pin":"1234"}`)

Example:

```bash
curl -X POST http://localhost:5000/security/PI1/arm ^
  -H "Content-Type: application/json" ^
  -d "{\"pin\":\"1234\"}"
```

## Configuration

Main configuration is in `config/settings.json`:

- MQTT broker, topic patterns, and QoS
- Batch publish parameters
- InfluxDB connection and buckets
- Security rules (PIN, delays, thresholds)
- Simulation flags and pin mapping for sensors/actuators

Note: some values can be overridden via environment variables (`MQTT_BROKER`, `MQTT_PORT`, `INFLUX_*`, `DEVICE_ID`, `DEVICE_NAME`).

## Project structure

```text
home-automation-project/
├── common/
├── config/
├── docker/
├── edge/
├── server/
├── webapp/
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```
