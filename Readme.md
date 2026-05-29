# Cold-Chain Logistics Risk Monitor

An automated ETL pipeline that fetches live weather forecasts for Louisville, KY
from the Open-Meteo API, assesses daily temperature and humidity risk for cold-chain
warehouses and cargo types, and loads all data into a PostgreSQL database for
Power BI dashboard consumption.

The pipeline uses an **incremental loading strategy** — seed tables are upserted on
every run, and only genuinely new forecast dates are inserted into `daily_forecasts`
and `risk_assessments`. Historical records are never overwritten or deleted, making
the pipeline safe to schedule as a daily cron job without manual intervention.

---

## Project Structure

```
cold_chain_risk_monitor/
├── data/                        ← CSV seed files (committed) + generated forecast
│   ├── warehouses.csv           ← Louisville cold-chain facilities
│   ├── cargo_types.csv          ← Cargo classifications with safe temp/humidity thresholds
│   ├── warehouse_cargo.csv      ← Bridge table — which warehouse stores which cargo
│   ├── weather_codes.csv        ← WMO weather code descriptions and metadata
│   └── daily_forecast.csv       ← Generated at runtime by run_pipeline.py (not committed)
├── sql/
│   └── create_views.sql         ← Power BI views (run once after first load)
├── run_pipeline.py              ← Complete ETL pipeline — single entry point
├── .env                         ← Your database credentials (not committed — see below)
├── .gitignore
├── requirements.txt
├── VALIDATION.md                ← Full data quality framework documentation
└── README.md
```

---

## Prerequisites

- Python 3.10 or higher
- A PostgreSQL database (local or Supabase)
- Power BI Desktop (for dashboard)

---

## Setup Instructions

### 1. Clone the repository

```bash
git clone https://github.com/your-username/cold-chain-risk-monitor.git
cd cold-chain-risk-monitor
```

### 2. Create and activate a virtual environment

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

Create a `.env` file in the project root. **Never commit this file.**

```env
user=your_db_user
password=your_db_password
host=your_db_host
port=5432
dbname=your_db_name
```

> **Supabase users:** your host is `db.<project-ref>.supabase.co`
> and `sslmode=require` is already set in the connection string.

---

## Running the Pipeline

The entire ETL workflow runs from a single script:

```bash
python run_pipeline.py
```

This executes all 10 pipeline steps in sequence:

1. Connect to PostgreSQL and verify credentials
2. Initialise schema (`CREATE TABLE IF NOT EXISTS` — safe to rerun)
3. Extract 7-day forecast from the Open-Meteo API
4. Transform and validate forecast data (V1–V7)
5. Save `data/daily_forecast.csv`
6. Load and validate all seed CSV files (V8–V12)
7. Upsert seed tables (locations, weather\_codes, warehouses, cargo\_types, warehouse\_cargo)
8. Incrementally insert new forecast rows + FK guard (V13)
9. Incrementally build and insert new risk assessments
10. Log a run summary

### Create Power BI views (run once after first load)

Connect to your PostgreSQL database and run:

```bash
psql -h your_host -U your_user -d your_db -f sql/create_views.sql
```

Or paste the contents of `sql/create_views.sql` directly into your database query
tool (e.g. Supabase SQL editor, DBeaver, pgAdmin).

---

## Incremental Loading Strategy

The pipeline uses a multi-strategy incremental approach rather than a full-refresh
drop-and-recreate pattern. This preserves the full history of loaded forecasts across
daily runs.

| Table | Strategy | Reason |
|---|---|---|
| `locations` | Upsert (`ON CONFLICT DO UPDATE`) | Stable seed — update if address changes |
| `weather_codes` | Upsert (`ON CONFLICT DO UPDATE`) | Stable lookup — update descriptions if corrected |
| `warehouses` | Upsert (`ON CONFLICT DO UPDATE`) | Stable seed — update if details change |
| `cargo_types` | Upsert (`ON CONFLICT DO UPDATE`) | Stable seed — update if thresholds change |
| `warehouse_cargo` | Upsert (`ON CONFLICT DO NOTHING`) | No non-key columns to update |
| `daily_forecasts` | Insert new dates only | Preserves historical forecast records |
| `risk_assessments` | Insert new dates only | Keeps full assessment history for trend analysis |

On a fresh database, all 7 days are inserted. On subsequent daily runs, only the one
new date that has rolled into the 7-day window is inserted — the other 6 are skipped.

---

## Environment Variables Reference

| Variable   | Description              | Default  |
|------------|--------------------------|----------|
| `user`     | PostgreSQL username       | required |
| `password` | PostgreSQL password       | required |
| `host`     | PostgreSQL host           | required |
| `port`     | PostgreSQL port           | `5432`   |
| `dbname`   | Database name             | required |

---

## Database Schema

```
locations
    └── daily_forecasts  (FK: location_id, weather_code_id)
    └── warehouses       (FK: location_id)
            └── warehouse_cargo  (FK: warehouse_id, cargo_type_id)
            └── risk_assessments (FK: warehouse_id, cargo_type_id, forecast_id)

weather_codes
    └── daily_forecasts  (FK: weather_code_id)

cargo_types
    └── warehouse_cargo  (FK: cargo_type_id)
    └── risk_assessments (FK: cargo_type_id)
```

---

## Risk Classification Logic

Each forecast day is scored per warehouse–cargo pair using the thresholds stored
in `cargo_types`. The overall `risk_level` is the higher of the two sub-scores.

| Condition | Risk Level |
|---|---|
| actual > safe\_max + 20 | Very High |
| actual > safe\_max + 10 | High |
| actual > safe\_max | Moderate |
| actual ≤ safe\_max | Low |
| NULL / invalid values | Unknown |

Each risk level maps to a specific `recommended_action` string stored in
`risk_assessments` for Power BI display.

---

## Validation Checks

The pipeline runs **13 data quality checks** across all stages. See
`VALIDATION.md` for full documentation of every check including what it tests,
why it matters, and how failures are handled.

| Check | Stage | Type | On Failure |
|---|---|---|---|
| V1 — API response validation | Extract | API response | Critical — halt |
| V2 — Null value check | Transform | Null | Critical — halt |
| V3 — Duplicate date detection | Transform | Duplicate | Critical — halt |
| V4 — Temperature range | Transform | Range | Warning — continue |
| V5 — Humidity range | Transform | Range | Warning — continue |
| V6 — Schema / column check | Transform | Schema | Critical — halt |
| V7 — Row count (7 days) | Transform | Row count | Warning — continue |
| V8 — Null check (all tables) | Load | Null | Critical — halt |
| V9 — Duplicate PK detection | Load | Duplicate | Critical — halt |
| V10 — Referential integrity | Load | RI | Critical — halt |
| V11 — Temp range re-check | Load | Range | Warning — continue |
| V12 — Seed table row counts | Load | Row count | Warning — continue |
| V13 — FK guard (weather codes) | Load | RI | Warning — auto-fix |

---

## Power BI Connection

1. Open Power BI Desktop
2. **Get Data → PostgreSQL database**
3. Enter your host and database name
4. Import these views: `vw_daily_forecast`, `vw_risk_assessments`,
   `vw_warehouse_summary`, `vw_executive_kpis`
5. Set relationships in Model view if not auto-detected
6. Publish to Power BI Service and schedule daily refresh

---

## Rerunning the Pipeline

The pipeline is fully safe to rerun at any time. The incremental strategy ensures:

- No data is deleted or overwritten
- Only new forecast dates are inserted
- Seed table changes (corrected CSVs) are picked up automatically via upsert
- The schema is created only if it does not already exist

```bash
python run_pipeline.py
```

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `Missing required file: warehouses.csv` | Seed CSV missing from `data/` | Ensure all seed CSVs are present in the `data/` folder |
| `Missing one or more required .env variables` | `.env` file missing or incomplete | Create `.env` with all 5 required variables |
| `Database connection failed` | Wrong credentials or host | Check `.env` values and network access |
| `V10 FAILED — orphaned FK values` | `warehouse_cargo.csv` references an ID not in warehouses or cargo_types | Fix the bridge CSV to only reference valid IDs |
| `V13 WARNING — missing weather codes` | Open-Meteo returned an unseen WMO code | Placeholder rows are auto-inserted; update `weather_codes.csv` for next run |
