# Validation Framework Documentation

**Cold-Chain Logistics Risk Monitor**
`run_pipeline.py` — Complete ETL Validation Reference

---

## 1. Overview

The Cold-Chain Logistics Risk Monitor ETL pipeline implements **13 numbered data quality checks** across every stage of execution — from API extraction through to database loading. All checks run from a single entry point: `run_pipeline.py`.

Checks are classified as either **Critical** (pipeline halts immediately on failure, preventing corrupt or incomplete data from reaching the database) or **Warning** (pipeline logs the issue and continues, allowing partial data to proceed with the problem flagged for review). One check (V13) uses an **auto-fix** strategy, inserting placeholder rows rather than halting or silently skipping.

---

## 2. Validation Check Summary

| Check | Description | Stage | Type | On Failure | Severity |
|---|---|---|---|---|---|
| **V1** | API response validation | Extract | API response | Critical — halt | 🔴 CRITICAL |
| **V2** | Null value check | Transform | Null | Critical — halt | 🔴 CRITICAL |
| **V3** | Duplicate date detection | Transform | Duplicate | Critical — halt | 🔴 CRITICAL |
| **V4** | Temperature range validation | Transform | Range | Warning — continue | 🟡 WARNING |
| **V5** | Humidity range validation | Transform | Range | Warning — continue | 🟡 WARNING |
| **V6** | Schema / column validation | Transform | Schema | Critical — halt | 🔴 CRITICAL |
| **V7** | Row count verification (7 days) | Transform | Row count | Warning — continue | 🟡 WARNING |
| **V8** | Null check on all source tables | Load | Null | Critical — halt | 🔴 CRITICAL |
| **V9** | Duplicate PK detection | Load | Duplicate | Critical — halt | 🔴 CRITICAL |
| **V10** | Referential integrity (warehouse_cargo) | Load | RI | Critical — halt | 🔴 CRITICAL |
| **V11** | Forecast temperature range re-check | Load | Range | Warning — continue | 🟡 WARNING |
| **V12** | Seed table row count check | Load | Row count | Warning — continue | 🟡 WARNING |
| **V13** | FK guard: weather codes | Load | RI | Warning — auto-fix | 🟢 AUTO-FIX |

> **Severity key:** 🔴 CRITICAL — pipeline halts &nbsp;|&nbsp; 🟡 WARNING — pipeline continues &nbsp;|&nbsp; 🟢 AUTO-FIX — placeholder inserted, pipeline continues

---

## 3. Incremental Loading Strategy

The pipeline uses a multi-strategy incremental approach rather than a full-refresh drop-and-recreate pattern. This preserves historical forecast and risk assessment records across daily runs, making the pipeline safe to schedule without manual intervention.

| Table | Strategy | Rationale |
|---|---|---|
| `locations` | Upsert (`ON CONFLICT DO UPDATE`) | Stable seed — picks up address or timezone corrections |
| `weather_codes` | Upsert (`ON CONFLICT DO UPDATE`) | Stable lookup — updates descriptions if codes are corrected |
| `warehouses` | Upsert (`ON CONFLICT DO UPDATE`) | Stable seed — updates facility details if changed |
| `cargo_types` | Upsert (`ON CONFLICT DO UPDATE`) | Stable seed — picks up threshold changes automatically |
| `warehouse_cargo` | Upsert (`ON CONFLICT DO NOTHING`) | Bridge table has no non-key columns to update; skip duplicates |
| `daily_forecasts` | Insert new dates only | Preserves historical forecast records for trend analysis |
| `risk_assessments` | Insert new dates only | Keeps full assessment history; avoids re-scoring past dates |

On a fresh database, all 7 forecast days are inserted. On subsequent daily runs, only the one new date that has rolled into the 7-day window is inserted — the other 6 dates already present in `daily_forecasts` are detected by `get_existing_forecast_dates()` and skipped before any `INSERT` is attempted.

---

## 4. Extract Stage Checks (V1)

These checks run inside `extract_weather_forecast()` before any transformation begins.

### V1 — API Response Validation

| | |
|---|---|
| **What it checks** | Confirms the Open-Meteo API returned at least one location response object before any data processing begins. |
| **Why it matters** | An empty response list means zero data was fetched. Every downstream step would silently produce an empty DataFrame that could then be used to generate empty or misleading risk assessments. |
| **On failure** | 🔴 Raises `ValueError` — pipeline halts immediately with a clear log message. No CSV is written and no database writes are attempted. |

---

## 5. Transform Stage Checks (V2 – V7)

These checks run inside `validate_forecast()` after `transform_forecast()` has shaped the data but before `save_forecast_csv()` writes the CSV or any database operation begins.

### V2 — Null Value Check

| | |
|---|---|
| **What it checks** | Counts null values in every column of the transformed forecast DataFrame. |
| **Why it matters** | Nulls in `forecast_date` or `weather_code_id` would violate `NOT NULL` and FK constraints in PostgreSQL. Nulls in temperature or humidity columns cause `classify_risk()` to return `'Unknown'` for every associated risk assessment row, rendering the risk engine useless for those days. |
| **On failure** | 🔴 Logged as `ERROR` — pipeline halts before the CSV is written. |

### V3 — Duplicate Date Detection

| | |
|---|---|
| **What it checks** | Checks for repeated `forecast_date` values in the transformed DataFrame. |
| **Why it matters** | The `daily_forecasts` table has a `UNIQUE` constraint on `forecast_date`. A duplicate would cause a PostgreSQL insert failure and roll back the affected rows. |
| **On failure** | 🔴 Logged as `ERROR` — pipeline halts. |

### V4 — Temperature Range Validation

| | |
|---|---|
| **What it checks** | Confirms `temp_max_f` and `temp_min_f` fall within a plausible real-world range of −30°F to 130°F. |
| **Why it matters** | A unit conversion error (e.g. Celsius returned instead of Fahrenheit) produces absurd values that trigger false Very High risk assessments across all cargo types for all 7 forecast days. |
| **On failure** | 🟡 Logged as `WARNING` — pipeline continues, but the affected rows are printed for review. |

### V5 — Humidity Range Validation

| | |
|---|---|
| **What it checks** | Confirms `humidity_avg_pct` is between 0 and 100. |
| **Why it matters** | Relative humidity outside 0–100% is physically impossible and indicates a data error. Values above 100 would incorrectly classify all cargo as high-humidity risk. |
| **On failure** | 🟡 Logged as `WARNING` — pipeline continues. |

### V6 — Schema / Column Validation

| | |
|---|---|
| **What it checks** | Verifies the transformed DataFrame contains exactly the 6 expected columns: `forecast_date`, `weather_code_id`, `temp_max_f`, `temp_min_f`, `humidity_avg_pct`, `precipitation_prob_pct`. |
| **Why it matters** | An API change or rename in the transform step could silently drop a column. The load step would then either insert `NULL`s for the missing column or crash with an unexpected column error, leaving the database in a partially loaded state. |
| **On failure** | 🔴 Missing columns → logged as `ERROR`, pipeline halts. 🟡 Extra columns → logged as `WARNING`, pipeline continues. |

### V7 — Row Count Verification

| | |
|---|---|
| **What it checks** | Confirms exactly 7 forecast rows were returned by the API. |
| **Why it matters** | Open-Meteo's default forecast window is 7 days. Fewer rows indicate a partial API response; more rows indicate unexpected behaviour. Either case would produce mismatched risk assessment counts. |
| **On failure** | 🟡 Logged as `WARNING` — pipeline continues. |

---

## 6. Load Stage Checks (V8 – V13)

These checks run inside `validate_source_data()` after all CSV files have been loaded into DataFrames but before any upsert or insert is attempted against the database.

### V8 — Null Check on Source Tables

| | |
|---|---|
| **What it checks** | Counts nulls in critical columns across all source DataFrames: `warehouses`, `cargo_types`, `warehouse_cargo`, and `forecast`. Critical columns checked include primary keys, cargo threshold columns (`temp_max_f`, `humidity_max_pct`), and `forecast_date`. |
| **Why it matters** | Nulls in PK columns violate `NOT NULL` constraints and crash the `INSERT`. Nulls in `temp_max_f` or `humidity_max_pct` in `cargo_types` cause `classify_risk()` to return `'Unknown'` for all rows associated with that cargo type. |
| **On failure** | 🔴 Logged as `ERROR` — pipeline halts before any table is loaded. |

### V9 — Duplicate Primary Key Detection

| | |
|---|---|
| **What it checks** | Checks for repeated PK values in `warehouses` (`warehouse_id`), `cargo_types` (`cargo_type_id`), and `forecast` (`forecast_date`). |
| **Why it matters** | Duplicate PKs violate `PRIMARY KEY` constraints and cause the entire table load to fail mid-insert, leaving the database in a partially loaded state. |
| **On failure** | 🔴 Logged as `ERROR` — pipeline halts. |

### V10 — Referential Integrity Check

| | |
|---|---|
| **What it checks** | Verifies every `warehouse_id` and `cargo_type_id` in `warehouse_cargo.csv` exists in the parent `warehouses` and `cargo_types` tables respectively. |
| **Why it matters** | Orphaned FK values in `warehouse_cargo` would violate `REFERENCES` constraints and crash the `warehouse_cargo` upsert. This check catches the error before any database write is attempted. |
| **On failure** | 🔴 Logged as `ERROR` — pipeline halts. |

### V11 — Forecast Temperature Range Re-check

| | |
|---|---|
| **What it checks** | Re-validates `temp_max_f` and `temp_min_f` on the forecast DataFrame after it has been prepared for loading by `build_forecast_for_load()`. |
| **Why it matters** | Provides a second, independent layer of defence in the load stage in case `daily_forecast.csv` was manually edited between runs or the transformation introduced unexpected values. |
| **On failure** | 🟡 Logged as `WARNING` — pipeline continues. |

### V12 — Seed Table Row Count Check

| | |
|---|---|
| **What it checks** | Confirms each static seed table (`warehouses`, `cargo_types`, `warehouse_cargo`) has at least 1 row. |
| **Why it matters** | An accidentally empty or truncated CSV loads silently, producing no warehouses, no cargo types, and therefore zero risk assessment rows — with no error message to explain why the risk engine produced no output. |
| **On failure** | 🟡 Logged as `WARNING` — pipeline continues. |

### V13 — FK Guard: Weather Codes

| | |
|---|---|
| **What it checks** | Confirms every `weather_code_id` present in the forecast DataFrame already exists in the `weather_codes` table before `daily_forecasts` is inserted. |
| **Why it matters** | Open-Meteo can return any valid WMO weather code. The `weather_codes.csv` seeds a fixed set of common codes. Any unseen code would cause a FK constraint violation and crash the `daily_forecasts` insert with no rows written. |
| **On failure** | 🟢 Logged as `WARNING` — placeholder rows are automatically upserted into `weather_codes` so the pipeline continues safely. Placeholders are flagged with `'description pending review'` for manual follow-up in the next CSV refresh. |

---

## 7. Risk Level Action Map

Each row in `risk_assessments` stores a `recommended_action` string derived from the overall `risk_level`. The mapping covers all five possible values returned by `classify_risk()`.

| Risk Level | Recommended Action |
|---|---|
| **Very High** | IMMEDIATE ACTION REQUIRED — halt warehouse intake/dispatch for this cargo type. Contact operations manager and initiate emergency temperature/humidity controls. |
| **High** | Escalate to operations manager. Inspect active cargo for signs of spoilage or degradation. Increase monitoring to every 30 minutes and prepare contingency cooling. |
| **Moderate** | Increase monitoring frequency. Verify refrigeration units are operating within spec and review cargo placement to minimise ambient exposure. |
| **Low** | Normal operations. Continue standard monitoring schedule. |
| **Unknown** | Risk could not be assessed — verify sensor data and cargo threshold records before resuming normal operations. |

---

## 8. Logging & Failure Reference

All validation output uses Python's standard `logging` module with timestamped, level-prefixed messages. The table below maps log levels to pipeline behaviour.

| Log Level | Meaning | Pipeline Behaviour |
|---|---|---|
| `INFO` | Check passed | Pipeline continues normally. Success message logged with `✓` prefix. |
| `WARNING` | Soft failure | Issue logged with full details. Pipeline continues. Flagged rows printed for manual review. |
| `ERROR` | Critical failure | Failure added to the `failures[]` list. After all checks complete, `RuntimeError` is raised and the pipeline halts before any database write. |
| `CRITICAL` | Unhandled exception | Top-level `except` block catches any unhandled exception, logs it at `CRITICAL` level, and calls `sys.exit(1)` so the process returns a non-zero exit code for scheduler detection. |
