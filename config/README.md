# Configuration

This folder contains the application settings file `settings.json` used by the project.

Key points:

- `config/settings.json` is the canonical structured config for the device and server. It contains sections for `device`, `mqtt`, `batch` and `server.influx`.
- Environment variables can be used to override a few runtime values:
  - `MQTT_BROKER` — override MQTT broker hostname (useful when running inside Docker vs on host)
  - `MQTT_PORT` — override MQTT port
- The `docker-compose.yml` uses a set of environment variables (see `.env.example`) to bootstrap InfluxDB and pass secrets (eg. `INFLUXDB_INIT_ADMIN_TOKEN`, `INFLUXDB_INIT_BUCKET`) to the containers.

Recommendations:
- Copy `.env.example` to `.env` and edit values before running `docker compose up` in a fresh environment.
- Prefer storing long-lived secrets (Influx tokens) in a secure place; `.env` is convenient for local development but not secure for production.

Examples

Start local compose using `.env` values (make sure `.env` is next to `docker-compose.yml`):

```sh
cp .env.example .env
# Edit `.env` and then:
docker compose up -d --build
```

Running the edge (device simulator) on the host:

```sh
# When running on host, you may want to point MQTT_BROKER=localhost
# and launch the simulator directly:
python -m edge.main
```

Running the server (Flask + Influx ingestion):

```sh
python -m server.app
```

If you'd like, I can also:
- add `.env` to `.gitignore`,
- wire reading `INFLUX_TOKEN` / `INFLUX_ORG` in `config/settings.py` (if you want env overrides for them), or
- create a CI check that validates `.env.example` contains required keys.

