# Home Automation Project

IoT sistem za nadzor i upravljanje kućnim uređajima sa MQTT komunikacijom, InfluxDB skladištenjem, Flask API-jem, Angular web aplikacijom i Grafana dashboard-om.

## Arhitektura

Projekat se sastoji od sledećih delova:

- **edge/** – simulacija ili rad sa stvarnim senzorima/aktuatorima (DS1, DPIR1, DUS1, DMS, WEBC, DL, DB), MQTT publish/subscribe logika.
- **server/** – Flask API + servis koji čita poruke sa MQTT-a, upisuje u InfluxDB i vodi bezbednosno stanje (arming/disarming/alarm).
- **webapp/** – Angular klijent za pregled stanja uređaja i kontrolu bezbednosnog režima.
- **docker-compose.yml** – podizanje infrastrukture i backend servisa (Mosquitto, InfluxDB, Grafana, app, device).

Tok podataka:
1. Edge publikuje senzorske podatke na MQTT.
2. Server preuzima poruke sa MQTT i upisuje ih u InfluxDB.
3. Web app čita stanje preko Flask API-ja.
4. Komande za aktuatore i bezbednosne akcije idu preko API-ja nazad na edge uređaj.

## Tehnologije

- Python 3.11
- Flask
- Eclipse Mosquitto (MQTT broker)
- InfluxDB 2.7
- Grafana
- Angular 21
- Docker / Docker Compose

## Pokretanje (preporučeno: Docker)

Preduslovi:
- Docker Desktop (sa Docker Compose podrškom)
- Node.js + npm (samo za webapp)

U root direktorijumu projekta pokreni:

```powershell
docker compose up -d --build mosquitto influxdb grafana app
docker compose run --rm --service-ports device
```

Na Windows-u možeš i:

```powershell
.\run-device-interactive.ps1
```

Servisi nakon pokretanja:

- Flask API: `http://localhost:5000`
- Grafana: `http://localhost:3000` (admin / admin)
- InfluxDB: `http://localhost:8086`
- MQTT broker: `localhost:1883`

## Pokretanje web aplikacije

Web app nije u docker-compose servisu, pa se pokreće lokalno:

```powershell
cd webapp
npm install
npm run start
```

UI je dostupan na `http://localhost:4200`.

## API rute

Osnovne rute servera:

- `GET /health` – health check
- `GET /status/<device_id>` – trenutno stanje senzora/aktuatora i security snapshot
- `POST /actuator/<device_id>/<actuator_type>` – slanje komande aktuatoru (npr. `led`, `buzzer`)
- `GET /security/<device_id>` – trenutno security stanje
- `POST /security/<device_id>/arm` – aktiviranje sistema (`{"pin":"1234"}`)
- `POST /security/<device_id>/disarm` – deaktiviranje sistema (`{"pin":"1234"}`)

Primer:

```bash
curl -X POST http://localhost:5000/security/PI1/arm ^
  -H "Content-Type: application/json" ^
  -d "{\"pin\":\"1234\"}"
```

## Konfiguracija

Glavna konfiguracija je u `config/settings.json`:

- MQTT broker, topic pattern-i i QoS
- Parametri batch publish-a
- InfluxDB konekcija i bucket-i
- Security pravila (PIN, kašnjenja, pragovi)
- Simulacija i pin mapiranje za senzore/aktuatora

Napomena: deo vrednosti može biti pregažen env promenljivama (`MQTT_BROKER`, `MQTT_PORT`, `INFLUX_*`, `DEVICE_ID`, `DEVICE_NAME`).

## Struktura projekta

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
