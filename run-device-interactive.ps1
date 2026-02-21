$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

docker compose up -d --build mosquitto influxdb grafana app
docker compose run --rm --service-ports device
