# Battery Telemetry Assessment
This project fetched battery telemetry data from the Intellicar Standard API, stores it in a SQLite db, collects distance travelled data, and is structured to support charging/discharging analysis and anomaly detection.

## What This Project Does
- Authenticates with the Intellicar API using username and password
- Fetches the list of available vehicles
- Collects latest CAN/battery telemetry for all vehicles
- Stores telemetry data in SQLite
- Collects distance travelled data for vehicles
- Stores distance data in SQLite
- Runs continuously:
  - telemetry collection every 30 seconds
  - distance collection every 5 minutes
- Handles per-vehicle API failures without crashing the full pipeline

## Project Structure

```text
battery-data-analysis/
├─ src/
│  ├─ __init__.py
│  ├─ api_client.py
│  ├─ collector.py
│  ├─ config.py
│  ├─ db.py
│  ├─ main.py
│  ├─ parsers.py
│  └─ utils.py
├─ data/
│  └─ battery_data.db
├─ output/
├─ .env
├─ .gitignore
├─ README.md
└─ requirements.txt
```

## Tech Stack
Python
SQLite
Requests
Pandas
python-dotenv