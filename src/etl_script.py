"""
Cold-Chain Logistics Risk Monitor
run_pipeline.py — Complete ETL Pipeline (Extract → Transform → Validate → Load)

Pulls a 7-day daily weather forecast from Open-Meteo for Louisville, KY,
transforms it to match the PostgreSQL schema, validates all data, and loads
every table into PostgreSQL using an INCREMENTAL strategy:

    • Seed tables (locations, weather_codes, warehouses, cargo_types,
      warehouse_cargo) are upserted — new rows are inserted; existing rows
      are updated in place, so reruns are safe and additive.

    • daily_forecasts is loaded incrementally — only forecast dates not
      already present in the database are inserted; existing dates are
      skipped to avoid UNIQUE constraint violations and data loss.

    • risk_assessments is loaded incrementally — rows for dates that already
      have assessments are skipped; only new forecast dates produce new rows.

Incremental loading rationale
------------------------------
A pure full-refresh strategy (DROP + recreate) was evaluated but rejected
because it destroys historical forecast data on every run. Since Open-Meteo
updates its rolling 7-day window daily, older dates that have already been
loaded should be preserved for trend analysis and Power BI history views.
Incremental loading retains all historical rows while appending only genuinely
new data, making the pipeline safe to schedule as a daily cron job without
manual intervention.

Run order (single entry point):
    python run_pipeline.py
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import openmeteo_requests
import pandas as pd
import requests_cache
from dotenv import load_dotenv
from retry_requests import retry
from sqlalchemy import create_engine, text
from sqlalchemy.types import Date, Float, Integer, SmallInteger, String


# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# =============================================================================
# CONFIG
# =============================================================================

# ---------------------------------------------------------------------------
# Locate the project-level data/ folder regardless of where run_pipeline.py
# is placed.  The script may sit at the project root OR inside a src/ sub-
# folder — both layouts are handled by walking up the directory tree until a
# folder named "data" is found, up to two levels above the script itself.
#
# Supported layouts:
#   cold_chain_risk_monitor/
#   ├── data/               ← data/ at project root  (run_pipeline.py here)
#   └── run_pipeline.py
#
#   cold_chain_risk_monitor/
#   ├── data/               ← data/ one level above src/
#   └── src/
#       └── run_pipeline.py
# ---------------------------------------------------------------------------

def _find_data_dir() -> Path:
    """
    Search for the project-level data/ folder starting from the directory
    that contains this script, then checking up to two parent directories.

    Validation: a candidate is only accepted if it contains at least one
    of the required seed CSV files (warehouses.csv).  This prevents an
    empty src/data/ folder left over from an old project layout from being
    picked up in preference to the real project-level data/ folder.

    Raises RuntimeError with a clear, actionable message if no valid
    data/ folder is found.
    """
    script_dir = Path(__file__).resolve().parent
    candidates = [
        script_dir / "data",                # script is at project root
        script_dir.parent / "data",         # script is inside src/
        script_dir.parent.parent / "data",  # script is two levels deep
    ]
    # A sentinel file that must exist in the real data/ folder.
    # An empty directory (e.g. a leftover src/data/ from old scripts) will
    # not contain this file and will therefore be skipped.
    sentinel = "warehouses.csv"
    for candidate in candidates:
        if candidate.is_dir() and (candidate / sentinel).is_file():
            return candidate
    raise RuntimeError(
        f"Cannot locate a valid data/ folder containing {sentinel}.\n"
        f"Searched:\n" +
        "\n".join(f"  {c}" for c in candidates) +
        "\n\nSteps to fix:\n"
        "  1. Ensure the data/ folder is at the project root (next to run_pipeline.py or next to src/).\n"
        "  2. Ensure warehouses.csv, cargo_types.csv, warehouse_cargo.csv, and weather_codes.csv are inside it.\n"
        "  3. If an empty src/data/ folder exists from old scripts, you can delete it — it is not used."
    )


BASE_DIR     = Path(__file__).resolve().parent
DATA_DIR     = _find_data_dir()
CACHE_DIR    = DATA_DIR / ".api_cache"
FORECAST_CSV = DATA_DIR / "daily_forecast.csv"

LATITUDE  = 38.2527   # Louisville, KY — matches locations seed record
LONGITUDE = -85.7585

CACHE_DIR.mkdir(parents=True, exist_ok=True)

# CSV seed file paths
WAREHOUSES_CSV      = DATA_DIR / "warehouses.csv"
CARGO_TYPES_CSV     = DATA_DIR / "cargo_types.csv"
WAREHOUSE_CARGO_CSV = DATA_DIR / "warehouse_cargo.csv"
WEATHER_CODES_CSV   = DATA_DIR / "weather_codes.csv"

# Expected columns in the transformed forecast DataFrame
EXPECTED_COLUMNS = [
    "forecast_date",
    "weather_code_id",
    "temp_max_f",
    "temp_min_f",
    "humidity_avg_pct",
    "precipitation_prob_pct",  # mean daily precipitation probability (0–100 %)
]

# Plausible physical bounds used in range validation
TEMP_MIN_PLAUSIBLE = -30.0   # °F
TEMP_MAX_PLAUSIBLE =  130.0  # °F
HUMIDITY_MIN       =    0.0
HUMIDITY_MAX       =  100.0

# Fixed precipitation probability thresholds for precip_risk scoring.
# These are operational bands, not cargo-specific regulatory limits —
# precipitation probability has no equivalent FDA/USDA threshold.
# Bands reflect the likelihood of rain/snow affecting loading, unloading,
# and dock-door temperature fluctuations on a given forecast day.
PRECIP_RISK_THRESHOLDS = {
    "Very High": 80,   # ≥ 80 % — near-certain precipitation event
    "High":      60,   # ≥ 60 % — likely precipitation
    "Moderate":  40,   # ≥ 40 % — possible precipitation
    # < 40 %  → Low
}

# Recommended actions mapped to every possible risk level
RISK_ACTION_MAP = {
    "Very High": (
        "IMMEDIATE ACTION REQUIRED — halt warehouse intake/dispatch for this cargo type. "
        "Contact operations manager and initiate emergency temperature/humidity controls."
    ),
    "High": (
        "Escalate to operations manager. Inspect active cargo for signs of spoilage or "
        "degradation. Increase monitoring to every 30 minutes and prepare contingency cooling."
    ),
    "Moderate": (
        "Increase monitoring frequency. Verify refrigeration units are operating within "
        "spec and review cargo placement to minimise ambient exposure."
    ),
    "Low": (
        "Normal operations. Continue standard monitoring schedule."
    ),
    "Unknown": (
        "Risk could not be assessed — verify sensor data and cargo threshold records "
        "before resuming normal operations."
    ),
}


# =============================================================================
# SECTION 1 — DATABASE CONNECTION
# =============================================================================

def get_engine():
    """
    Build and test the PostgreSQL connection from .env variables.
    Required keys: user, password, host, port, dbname.
    Raises RuntimeError immediately if any variable is missing or the
    connection cannot be established (fail-fast pattern).
    """
    load_dotenv()

    user     = os.getenv("user")
    password = os.getenv("password")
    host     = os.getenv("host")
    port     = os.getenv("port")
    dbname   = os.getenv("dbname")

    if not all([user, password, host, port, dbname]):
        raise RuntimeError(
            "Missing one or more required .env variables: user, password, host, port, dbname"
        )

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}?sslmode=require"

    try:
        engine = create_engine(url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✓ Database connection successful")
        return engine
    except Exception as exc:
        raise RuntimeError(f"Database connection failed: {exc}") from exc


# =============================================================================
# SECTION 2 — SCHEMA INITIALISATION (incremental-safe)
# =============================================================================

def create_schema(engine) -> None:
    """
    Create all tables using CREATE TABLE IF NOT EXISTS so the schema
    initialisation is fully idempotent.  No data is ever dropped on a
    rerun — new rows are merged in by the upsert / incremental helpers
    below.  Tables are created in FK-safe dependency order.
    """
    logger.info("Initialising database schema (CREATE IF NOT EXISTS)...")

    create_sql = """
    CREATE TABLE IF NOT EXISTS locations (
        location_id   INT PRIMARY KEY,      -- explicit value supplied by pipeline (not SERIAL)
        city          VARCHAR(100),
        state         VARCHAR(50),
        latitude      NUMERIC(8,5),
        longitude     NUMERIC(8,5),
        timezone      VARCHAR(100)
    );

    CREATE TABLE IF NOT EXISTS weather_codes (
        weather_code_id  SMALLINT PRIMARY KEY,
        description      VARCHAR(255) NOT NULL,
        icon             VARCHAR(10),
        severity_level   SMALLINT,
        category         VARCHAR(50),
        color_hex        VARCHAR(7)
    );

    CREATE TABLE IF NOT EXISTS daily_forecasts (
        forecast_id       SERIAL PRIMARY KEY,
        location_id       INT      REFERENCES locations(location_id),
        weather_code_id   SMALLINT REFERENCES weather_codes(weather_code_id),
        forecast_date     DATE     UNIQUE NOT NULL,
        temp_max_f        NUMERIC(5,2),
        temp_min_f        NUMERIC(5,2),
        humidity_avg_pct  NUMERIC(5,2),
        precipitation_prob_pct NUMERIC(5,2)  -- mean daily precipitation probability (0-100 %)
    );

    CREATE TABLE IF NOT EXISTS warehouses (
        warehouse_id    INT PRIMARY KEY,
        warehouse_name  VARCHAR(255),
        street_address  VARCHAR(255),
        location_id     INT REFERENCES locations(location_id)
    );

    CREATE TABLE IF NOT EXISTS cargo_types (
        cargo_type_id    INT PRIMARY KEY,
        cargo_name       VARCHAR(255),
        temp_min_f       NUMERIC(5,2),
        temp_max_f       NUMERIC(5,2),
        humidity_min_pct NUMERIC(5,2),
        humidity_max_pct NUMERIC(5,2),
        regulatory_body  VARCHAR(100)
    );

    CREATE TABLE IF NOT EXISTS warehouse_cargo (
        warehouse_id  INT REFERENCES warehouses(warehouse_id),
        cargo_type_id INT REFERENCES cargo_types(cargo_type_id),
        PRIMARY KEY (warehouse_id, cargo_type_id)
    );

    CREATE TABLE IF NOT EXISTS risk_assessments (
        risk_id            SERIAL PRIMARY KEY,
        warehouse_id       INT  REFERENCES warehouses(warehouse_id),
        cargo_type_id      INT  REFERENCES cargo_types(cargo_type_id),
        forecast_id        INT  REFERENCES daily_forecasts(forecast_id),
        forecast_date      DATE,
        temp_risk          VARCHAR(20),
        humidity_risk      VARCHAR(20),
        precip_risk        VARCHAR(20),   -- precipitation probability risk (fixed operational bands)
        risk_level         VARCHAR(20),
        recommended_action TEXT
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_sql))

    logger.info("✓ Schema ready")


# =============================================================================
# SECTION 3 — EXTRACT  (Open-Meteo API)
# =============================================================================

def extract_weather_forecast() -> pd.DataFrame:
    """
    Fetch a 7-day daily forecast from Open-Meteo for Louisville, KY.

    Validation applied:
        V1 — API response validation: raises if no response is returned.

    Returns a raw DataFrame with original API column names.
    """
    logger.info("Connecting to Open-Meteo API...")

    cache_session = requests_cache.CachedSession(
        str(CACHE_DIR / "openmeteo_cache"),
        expire_after=3600,
    )
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo     = openmeteo_requests.Client(session=retry_session)

    url    = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude":         LATITUDE,
        "longitude":        LONGITUDE,
        "daily": [
            "weather_code",               # Variables(0)
            "temperature_2m_max",         # Variables(1)
            "temperature_2m_min",         # Variables(2)
            "precipitation_probability_mean",  # Variables(3)
            "relative_humidity_2m_mean",  # Variables(4)
        ],
        "timezone":         "auto",
        "temperature_unit": "fahrenheit",
    }

    logger.info("Requesting forecast for %.4f, %.4f", LATITUDE, LONGITUDE)

    try:
        responses = openmeteo.weather_api(url, params=params)
    except Exception as exc:
        logger.error("Open-Meteo API request failed: %s", exc)
        raise

    # -------------------------------------------------
    # V1 — API response validation
    # What:  Confirm the API returned at least one location response.
    # Why:   An empty response list means no data was fetched; every
    #        downstream step would silently produce an empty DataFrame.
    # Fails: Raises ValueError — pipeline halts with a clear message.
    # -------------------------------------------------
    if not responses:
        raise ValueError("V1 FAILED — No response received from Open-Meteo API.")

    response = responses[0]
    logger.info(
        "Received: %.4f°N %.4f°E  |  Timezone: %s  |  Elevation: %.1fm",
        response.Latitude(),
        response.Longitude(),
        response.Timezone().decode(),
        response.Elevation(),
    )

    daily = response.Daily()

    date_range = pd.date_range(
        start     = pd.to_datetime(daily.Time(),    unit="s", utc=True),
        end       = pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
        freq      = pd.Timedelta(seconds=daily.Interval()),
        inclusive = "left",
    ).tz_convert(response.Timezone().decode())

    raw_df = pd.DataFrame({
        "date":                      date_range,
        "weather_code":              daily.Variables(0).ValuesAsNumpy(),
        "temperature_2m_max":        daily.Variables(1).ValuesAsNumpy(),
        "temperature_2m_min":        daily.Variables(2).ValuesAsNumpy(),
        "precipitation_probability_mean": daily.Variables(3).ValuesAsNumpy(),
        "relative_humidity_2m_mean": daily.Variables(4).ValuesAsNumpy(),
    })

    logger.info("✓ Fetched %d forecast days from Open-Meteo", len(raw_df))
    return raw_df


# =============================================================================
# SECTION 4 — TRANSFORM
# =============================================================================

def transform_forecast(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename, cast, round, and select columns to match the daily_forecasts
    PostgreSQL schema.

    weather_description / icon / category / color_hex are intentionally
    excluded — they are attributes of weather_code_id and live in the
    weather_codes table (3NF). The views in create_views.sql JOIN them
    back for Power BI.
    """
    logger.info("Transforming raw forecast data...")
    df = raw_df.copy()

    # Strip timezone → plain YYYY-MM-DD string for CSV portability
    df["forecast_date"] = (
        df["date"]
        .dt.tz_localize(None)
        .dt.date
        .astype(str)
    )

    df = df.rename(columns={
        "weather_code":              "weather_code_id",
        "temperature_2m_max":        "temp_max_f",
        "temperature_2m_min":        "temp_min_f",
        "relative_humidity_2m_mean": "humidity_avg_pct",
        "precipitation_probability_mean": "precipitation_prob_pct",
    })

    # Cast to int16 — matches SMALLINT FK in weather_codes table
    df["weather_code_id"] = df["weather_code_id"].astype("int16")

    # Round to 2 dp — matches NUMERIC(5,2) / NUMERIC(6,2) column types
    for col in ("temp_max_f", "temp_min_f", "humidity_avg_pct", "precipitation_prob_pct"):
        df[col] = df[col].round(2)

    df = df[EXPECTED_COLUMNS]

    # Deduplicate dates (defensive guard against cached re-runs)
    before = len(df)
    df = df.drop_duplicates(subset=["forecast_date"], keep="first").reset_index(drop=True)
    if len(df) < before:
        logger.warning("Removed %d duplicate forecast date(s)", before - len(df))

    logger.info("✓ Transform complete — %d rows", len(df))
    return df


def save_forecast_csv(df: pd.DataFrame) -> None:
    """Persist the validated forecast to data/daily_forecast.csv."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(FORECAST_CSV, index=False)
    logger.info("✓ Saved %d rows to %s", len(df), FORECAST_CSV)


# =============================================================================
# SECTION 5 — VALIDATION  (V1–V13)
# =============================================================================

def validate_forecast(df: pd.DataFrame) -> None:
    """
    Run data quality checks on the transformed forecast DataFrame.
    Logs WARNING for soft failures and raises RuntimeError for critical
    failures, halting the pipeline before any bad data reaches the DB.

    Checks:
        V2 — Null value check
        V3 — Duplicate date detection
        V4 — Temperature range validation
        V5 — Humidity range validation
        V6 — Schema / column validation
        V7 — Row count verification
    """
    logger.info("Running forecast validation checks...")
    failures = []

    # -------------------------------------------------
    # V2 — Null value check
    # What:  Counts null values in every column.
    # Why:   Nulls in forecast_date or weather_code_id would break FK
    #        constraints and date-keyed joins in Power BI views.
    # Fails: Critical — pipeline halts.
    # -------------------------------------------------
    null_counts = df.isnull().sum()
    cols_with_nulls = null_counts[null_counts > 0]
    if not cols_with_nulls.empty:
        for col, count in cols_with_nulls.items():
            logger.error("V2 FAILED — %d null value(s) in column '%s'", count, col)
            failures.append(f"Nulls in {col}")
    else:
        logger.info("  ✓ V2 Null check passed — no nulls found")

    # -------------------------------------------------
    # V3 — Duplicate date detection
    # What:  Checks for repeated forecast_date values.
    # Why:   daily_forecasts.forecast_date has a UNIQUE constraint;
    #        duplicates cause a PostgreSQL insert failure.
    # Fails: Critical — pipeline halts.
    # -------------------------------------------------
    dupes = df["forecast_date"].duplicated().sum()
    if dupes > 0:
        logger.error("V3 FAILED — %d duplicate forecast_date(s) found", dupes)
        failures.append("Duplicate forecast_dates")
    else:
        logger.info("  ✓ V3 Duplicate check passed — all dates unique")

    # -------------------------------------------------
    # V4 — Temperature range validation
    # What:  Confirms temp_max_f and temp_min_f fall within a
    #        plausible real-world range (–30°F to 130°F).
    # Why:   API unit mismatches produce absurd values that trigger
    #        false Very High risk assessments across all cargo types.
    # Fails: Warning — pipeline continues but flags the rows.
    # -------------------------------------------------
    bad_temp = df[
        (df["temp_max_f"] < TEMP_MIN_PLAUSIBLE) |
        (df["temp_max_f"] > TEMP_MAX_PLAUSIBLE) |
        (df["temp_min_f"] < TEMP_MIN_PLAUSIBLE) |
        (df["temp_min_f"] > TEMP_MAX_PLAUSIBLE)
    ]
    if not bad_temp.empty:
        logger.warning(
            "V4 WARNING — %d row(s) with out-of-range temperatures:\n%s",
            len(bad_temp),
            bad_temp[["forecast_date", "temp_max_f", "temp_min_f"]].to_string(index=False),
        )
    else:
        logger.info("  ✓ V4 Temperature range check passed")

    # -------------------------------------------------
    # V5 — Humidity range validation
    # What:  Confirms humidity_avg_pct is between 0 and 100.
    # Why:   Values outside 0–100% are physically impossible and corrupt
    #        risk scoring against cargo thresholds.
    # Fails: Warning — pipeline continues.
    # -------------------------------------------------
    bad_humidity = df[
        (df["humidity_avg_pct"] < HUMIDITY_MIN) |
        (df["humidity_avg_pct"] > HUMIDITY_MAX)
    ]
    if not bad_humidity.empty:
        logger.warning(
            "V5 WARNING — %d row(s) with invalid humidity values:\n%s",
            len(bad_humidity),
            bad_humidity[["forecast_date", "humidity_avg_pct"]].to_string(index=False),
        )
    else:
        logger.info("  ✓ V5 Humidity range check passed")

    # -------------------------------------------------
    # V6 — Schema / column validation
    # What:  Verifies the DataFrame contains exactly the expected columns.
    # Why:   A column rename or API change could silently drop a field,
    #        causing the load to insert NULLs or crash on missing cols.
    # Fails: Critical — pipeline halts.
    # -------------------------------------------------
    missing_cols = set(EXPECTED_COLUMNS) - set(df.columns)
    extra_cols   = set(df.columns) - set(EXPECTED_COLUMNS)
    if missing_cols:
        logger.error("V6 FAILED — Missing expected columns: %s", missing_cols)
        failures.append(f"Missing columns: {missing_cols}")
    if extra_cols:
        logger.warning("V6 WARNING — Unexpected extra columns found: %s", extra_cols)
    if not missing_cols:
        logger.info("  ✓ V6 Schema check passed — all expected columns present")

    # -------------------------------------------------
    # V7 — Row count verification
    # What:  Confirms exactly 7 forecast rows were returned.
    # Why:   Fewer rows = partial API response; more = unexpected data.
    # Fails: Warning — pipeline continues.
    # -------------------------------------------------
    if len(df) != 7:
        logger.warning("V7 WARNING — Expected 7 forecast rows, got %d", len(df))
    else:
        logger.info("  ✓ V7 Row count check passed — 7 forecast days present")

    if failures:
        raise RuntimeError(
            f"Forecast validation failed — {len(failures)} critical check(s): {failures}"
        )

    logger.info("✓ All forecast validation checks passed")


def validate_source_data(
    warehouses: pd.DataFrame,
    cargo: pd.DataFrame,
    wc: pd.DataFrame,
    forecast: pd.DataFrame,
) -> None:
    """
    Run data quality checks on all source DataFrames before loading.

    Checks:
        V8  — Null check on all source tables
        V9  — Duplicate PK detection
        V10 — Referential integrity: warehouse_cargo FKs vs parent tables
        V11 — Forecast temperature range validation
        V12 — Row count verification on static seed tables
    """
    logger.info("Running source data validation...")
    failures = []

    # -------------------------------------------------
    # V8 — Null check on all source tables
    # What:  Counts nulls in PK and critical columns across all DataFrames.
    # Why:   Nulls in PK columns violate NOT NULL constraints and crash
    #        the INSERT; nulls in cargo thresholds break risk scoring.
    # Fails: Critical — pipeline halts.
    # -------------------------------------------------
    critical_cols = {
        "warehouses":      ["warehouse_id", "warehouse_name"],
        "cargo_types":     ["cargo_type_id", "cargo_name", "temp_max_f", "humidity_max_pct"],
        "warehouse_cargo": ["warehouse_id", "cargo_type_id"],
        "forecast":        ["forecast_date", "weather_code_id", "temp_max_f", "humidity_avg_pct"],
    }
    frames = {
        "warehouses": warehouses, "cargo_types": cargo,
        "warehouse_cargo": wc,    "forecast": forecast,
    }
    for name, cols in critical_cols.items():
        df = frames[name]
        for col in cols:
            if col in df.columns:
                nulls = df[col].isnull().sum()
                if nulls > 0:
                    logger.error("V8 FAILED — %d null(s) in %s.%s", nulls, name, col)
                    failures.append(f"Nulls in {name}.{col}")
    if not failures:
        logger.info("  ✓ V8 Null check passed across all source tables")

    # -------------------------------------------------
    # V9 — Duplicate PK detection
    # What:  Checks for repeated primary key values in each table.
    # Why:   Duplicate PKs violate PRIMARY KEY constraints and either
    #        crash the load or silently overwrite rows.
    # Fails: Critical — pipeline halts.
    # -------------------------------------------------
    pk_checks = [
        ("warehouses",  warehouses, "warehouse_id"),
        ("cargo_types", cargo,      "cargo_type_id"),
        ("forecast",    forecast,   "forecast_date"),
    ]
    for name, df, pk_col in pk_checks:
        if pk_col in df.columns:
            dupes = df[pk_col].duplicated().sum()
            if dupes > 0:
                logger.error("V9 FAILED — %d duplicate %s value(s) in %s", dupes, pk_col, name)
                failures.append(f"Duplicate {pk_col} in {name}")
            else:
                logger.info("  ✓ V9 No duplicate %s in %s", pk_col, name)

    # -------------------------------------------------
    # V10 — Referential integrity check
    # What:  Verifies every warehouse_id and cargo_type_id in
    #        warehouse_cargo exists in the parent tables.
    # Why:   Orphaned FK values violate REFERENCES constraints and crash
    #        the warehouse_cargo INSERT.
    # Fails: Critical — pipeline halts.
    # -------------------------------------------------
    wh_ids    = set(warehouses["warehouse_id"].dropna().astype(int))
    cargo_ids = set(cargo["cargo_type_id"].dropna().astype(int))

    orphan_wh = set(wc["warehouse_id"].dropna().astype(int)) - wh_ids
    orphan_ct = set(wc["cargo_type_id"].dropna().astype(int)) - cargo_ids

    if orphan_wh:
        logger.error("V10 FAILED — warehouse_cargo has unknown warehouse_id(s): %s", orphan_wh)
        failures.append(f"Orphan warehouse_ids: {orphan_wh}")
    else:
        logger.info("  ✓ V10 All warehouse_cargo.warehouse_id values exist in warehouses")

    if orphan_ct:
        logger.error("V10 FAILED — warehouse_cargo has unknown cargo_type_id(s): %s", orphan_ct)
        failures.append(f"Orphan cargo_type_ids: {orphan_ct}")
    else:
        logger.info("  ✓ V10 All warehouse_cargo.cargo_type_id values exist in cargo_types")

    # -------------------------------------------------
    # V11 — Forecast temperature range validation
    # What:  Confirms temp_max_f and temp_min_f are plausible.
    # Why:   Out-of-range values indicate a unit conversion error and
    #        produce incorrect risk assessments for all cargo.
    # Fails: Warning — pipeline continues.
    # -------------------------------------------------
    if "temp_max_f" in forecast.columns and "temp_min_f" in forecast.columns:
        bad = forecast[
            (forecast["temp_max_f"] < TEMP_MIN_PLAUSIBLE) |
            (forecast["temp_max_f"] > TEMP_MAX_PLAUSIBLE) |
            (forecast["temp_min_f"] < TEMP_MIN_PLAUSIBLE) |
            (forecast["temp_min_f"] > TEMP_MAX_PLAUSIBLE)
        ]
        if not bad.empty:
            logger.warning("V11 WARNING — %d row(s) with out-of-range temperatures", len(bad))
        else:
            logger.info("  ✓ V11 Forecast temperature range check passed")

    # -------------------------------------------------
    # V12 — Row count verification on static seed tables
    # What:  Confirms seed tables have the expected minimum row counts.
    # Why:   A truncated CSV loads silently but produces incomplete risk
    #        assessments or missing FK targets.
    # Fails: Warning — pipeline continues.
    # -------------------------------------------------
    min_counts  = {"warehouses": 1, "cargo_types": 1, "warehouse_cargo": 1}
    count_frames = {"warehouses": warehouses, "cargo_types": cargo, "warehouse_cargo": wc}
    for name, min_count in min_counts.items():
        actual = len(count_frames[name])
        if actual < min_count:
            logger.warning(
                "V12 WARNING — %s has only %d rows (expected >= %d)", name, actual, min_count
            )
        else:
            logger.info("  ✓ V12 %s row count OK (%d rows)", name, actual)

    if failures:
        raise RuntimeError(
            f"Source data validation failed — {len(failures)} critical check(s): {failures}"
        )

    logger.info("✓ All source data validation checks passed")


# =============================================================================
# SECTION 6 — LOAD HELPERS
# =============================================================================

def load_source_files():
    """
    Verify all required CSV files exist then load them into DataFrames.
    Raises FileNotFoundError immediately if any file is missing.
    """
    logger.info("Loading source CSV files...")

    required = [
        ("warehouses.csv",      WAREHOUSES_CSV),
        ("cargo_types.csv",     CARGO_TYPES_CSV),
        ("warehouse_cargo.csv", WAREHOUSE_CARGO_CSV),
        ("weather_codes.csv",   WEATHER_CODES_CSV),
    ]
    for name, path in required:
        if not path.exists():
            raise FileNotFoundError(
                f"Missing required file: {name}\nExpected at: {path}"
            )

    warehouses    = pd.read_csv(WAREHOUSES_CSV)
    cargo         = pd.read_csv(CARGO_TYPES_CSV)
    wc            = pd.read_csv(WAREHOUSE_CARGO_CSV)
    weather_codes = pd.read_csv(WEATHER_CODES_CSV)

    logger.info("  ✓ warehouses:      %d rows", len(warehouses))
    logger.info("  ✓ cargo_types:     %d rows", len(cargo))
    logger.info("  ✓ warehouse_cargo: %d rows", len(wc))
    logger.info("  ✓ weather_codes:   %d rows", len(weather_codes))

    return warehouses, cargo, wc, weather_codes


def build_locations() -> pd.DataFrame:
    """
    Single Louisville, KY location record.

    location_id is supplied explicitly (value 1) rather than relying on
    SERIAL auto-increment.  The locations table is defined with INT PRIMARY KEY
    (not SERIAL) so this value is always predictable and the upsert is safe to
    repeat on every pipeline run without sequence drift.
    """
    return pd.DataFrame([{
        "location_id": 1,
        "city":        "Louisville",
        "state":       "KY",
        "latitude":    38.2527,
        "longitude":   -85.7585,
        "timezone":    "America/New_York",
    }])


def get_location_id(engine) -> int:
    """
    Query the database for the location_id of the Louisville, KY record
    and return it as an integer.

    Why query instead of hardcoding 1?
    ------------------------------------
    Even though build_locations() always inserts location_id=1, defensive
    practice is to read the value back from the database after the upsert.
    This guarantees that every downstream FK (daily_forecasts.location_id
    and warehouses.location_id) uses the ID that actually exists in the
    locations table, rather than assuming the insert succeeded with the
    expected value.

    Raises RuntimeError if no Louisville record is found — this would mean
    the locations upsert failed silently, which should never happen but is
    worth catching explicitly before FK-dependent tables are loaded.
    """
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT location_id FROM locations WHERE city = 'Louisville' AND state = 'KY'")
        )
        row = result.fetchone()

    if row is None:
        raise RuntimeError(
            "Location record for Louisville, KY not found in the database after upsert. "
            "Check that the locations table was written correctly in Step 7."
        )

    location_id = int(row.location_id)
    logger.info("  ✓ Resolved location_id = %d (Louisville, KY)", location_id)
    return location_id


def build_forecast_for_load(df: pd.DataFrame, location_id: int) -> pd.DataFrame:
    """
    Convert forecast_date to Python date objects and attach the location_id FK.

    location_id is passed in as a parameter (queried from the DB by
    get_location_id) rather than hardcoded, so the FK value is always
    consistent with what is actually stored in the locations table.
    """
    df = df.copy()
    df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date
    df["location_id"]   = location_id
    return df


# =============================================================================
# SECTION 7 — INCREMENTAL LOAD FUNCTIONS
# =============================================================================

def upsert_table(df: pd.DataFrame, table: str, pk_col: str, engine) -> None:
    """
    Incrementally upsert seed table rows using INSERT … ON CONFLICT DO UPDATE.

    Strategy
    ---------
    Each seed table (locations, weather_codes, warehouses, cargo_types) has a
    stable primary key that does not change between runs.  On every pipeline
    execution we want to:
        • Insert rows whose PK is not yet in the database.
        • Update rows whose PK already exists, in case the source CSV was
          corrected (e.g. a warehouse address was fixed).

    This makes seed loading fully idempotent without ever deleting data.
    """
    if df.empty:
        logger.warning("Skipping upsert for %s — DataFrame is empty", table)
        return

    cols        = list(df.columns)
    col_list    = ", ".join(cols)
    placeholder = ", ".join([f":{c}" for c in cols])
    update_set  = ", ".join(
        [f"{c} = EXCLUDED.{c}" for c in cols if c != pk_col]
    )

    sql = text(f"""
        INSERT INTO {table} ({col_list})
        VALUES ({placeholder})
        ON CONFLICT ({pk_col}) DO UPDATE SET {update_set}
    """)

    with engine.begin() as conn:
        conn.execute(sql, df.to_dict(orient="records"))

    logger.info("  ✓ Upserted %d rows into %s", len(df), table)


def upsert_warehouse_cargo(df: pd.DataFrame, engine) -> None:
    """
    Incrementally upsert the bridge table using its composite PK.

    warehouse_cargo has no non-key columns to update, so ON CONFLICT DO
    NOTHING is the correct strategy — we just skip rows that already exist.
    """
    if df.empty:
        logger.warning("Skipping upsert for warehouse_cargo — DataFrame is empty")
        return

    sql = text("""
        INSERT INTO warehouse_cargo (warehouse_id, cargo_type_id)
        VALUES (:warehouse_id, :cargo_type_id)
        ON CONFLICT (warehouse_id, cargo_type_id) DO NOTHING
    """)

    with engine.begin() as conn:
        conn.execute(sql, df[["warehouse_id", "cargo_type_id"]].to_dict(orient="records"))

    logger.info("  ✓ Upserted %d rows into warehouse_cargo", len(df))


def get_existing_forecast_dates(engine) -> set:
    """
    Query the database for forecast_date values already loaded into
    daily_forecasts.  Used to filter out dates before insertion so we
    never attempt to insert a duplicate and never overwrite history.
    """
    with engine.connect() as conn:
        result = conn.execute(text("SELECT forecast_date FROM daily_forecasts"))
        return {row.forecast_date for row in result}


def insert_new_forecasts(df: pd.DataFrame, engine) -> int:
    """
    Incremental insert for daily_forecasts.

    Strategy
    ---------
    Open-Meteo returns the same rolling 7-day window on every run.  Days
    that were already loaded on a previous run must be skipped to avoid
    violating the UNIQUE constraint on forecast_date.  We do NOT use
    ON CONFLICT DO UPDATE here because forecast records should be treated
    as immutable once loaded — the pipeline is designed to capture the
    forecast as it was on the day it was first fetched, preserving a
    historical record for trend analysis.

    Only genuinely new dates (not yet in daily_forecasts) are inserted.
    Returns the number of rows actually inserted.
    """
    existing_dates = get_existing_forecast_dates(engine)
    new_rows = df[~df["forecast_date"].isin(existing_dates)].copy()

    if new_rows.empty:
        logger.info("  — No new forecast dates to insert (all %d already loaded)", len(df))
        return 0

    skipped = len(df) - len(new_rows)
    if skipped:
        logger.info("  — Skipping %d forecast date(s) already in database", skipped)

    dtype_map = {
        "weather_code_id":   SmallInteger(),
        "location_id":       Integer(),
        "temp_max_f":        Float(),
        "temp_min_f":        Float(),
        "humidity_avg_pct":  Float(),
        "precipitation_prob_pct": Float(),
        "forecast_date":     Date(),
    }

    new_rows.to_sql(
        "daily_forecasts",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
        dtype=dtype_map,
    )

    logger.info("  ✓ Inserted %d new forecast row(s) into daily_forecasts", len(new_rows))
    return len(new_rows)


def get_existing_assessment_dates(engine) -> set:
    """
    Query the database for forecast_date values already present in
    risk_assessments.  Used to avoid duplicating assessments for dates
    that have already been processed on a previous pipeline run.
    """
    with engine.connect() as conn:
        result = conn.execute(text("SELECT DISTINCT forecast_date FROM risk_assessments"))
        return {row.forecast_date for row in result}


# =============================================================================
# SECTION 8 — FK GUARD  (V13)
# =============================================================================

def guard_weather_codes(forecast_df: pd.DataFrame, engine) -> None:
    """
    Ensure every weather_code_id in the forecast exists in weather_codes.

    Validation check:
        V13 — Referential integrity: forecast weather codes exist in lookup

    If Open-Meteo returns a WMO code absent from weather_codes.csv, the FK
    constraint on daily_forecasts.weather_code_id would crash the load.
    This guard inserts placeholder rows so the pipeline proceeds safely and
    flags the new codes for manual review.
    """
    forecast_codes = set(forecast_df["weather_code_id"].dropna().astype(int).unique())

    with engine.connect() as conn:
        result       = conn.execute(text("SELECT weather_code_id FROM weather_codes"))
        stored_codes = {row.weather_code_id for row in result}

    missing = forecast_codes - stored_codes

    if not missing:
        logger.info("  ✓ V13 All forecast weather_code_ids exist in weather_codes table")
        return

    logger.warning(
        "V13 WARNING — %d weather code(s) missing from lookup table: %s — inserting placeholders",
        len(missing), missing,
    )

    placeholders = pd.DataFrame([
        {
            "weather_code_id": int(code),
            "description":     f"WMO code {code} — description pending review",
            "icon":            "🌡️",
            "severity_level":  2,
            "category":        "Other",
            "color_hex":       "#808080",
        }
        for code in sorted(missing)
    ])

    upsert_table(placeholders, "weather_codes", "weather_code_id", engine)
    logger.info("  ✓ Inserted %d placeholder weather code(s)", len(placeholders))


# =============================================================================
# SECTION 9 — RISK ENGINE
# =============================================================================

def classify_risk(actual, safe_max) -> str:
    """
    Classify risk level by how far actual value exceeds the safe maximum.

    Bands:
        Very High  — exceeds safe_max by more than 20 units
        High       — exceeds safe_max by more than 10 units
        Moderate   — exceeds safe_max by any amount
        Low        — within safe limit
        Unknown    — actual or safe_max is null / non-numeric
    """
    if pd.isna(actual) or pd.isna(safe_max):
        return "Unknown"
    try:
        actual_val = float(actual)
        safe_val   = float(safe_max)
        if actual_val > safe_val + 20:
            return "Very High"
        elif actual_val > safe_val + 10:
            return "High"
        elif actual_val > safe_val:
            return "Moderate"
        return "Low"
    except (ValueError, TypeError):
        return "Unknown"


def classify_precip_risk(precip_prob) -> str:
    """
    Classify precipitation risk using fixed operational threshold bands.

    Unlike classify_risk(), this function does NOT compare against a
    cargo-specific safe maximum — there is no regulatory standard for a
    maximum safe precipitation probability.  Instead, fixed bands reflect
    the operational likelihood of rain or snow affecting loading, unloading,
    and dock-door temperature fluctuations.

    Bands (from PRECIP_RISK_THRESHOLDS):
        Very High  — ≥ 80 %  near-certain precipitation event
        High       — ≥ 60 %  likely precipitation
        Moderate   — ≥ 40 %  possible precipitation
        Low        — < 40 %  unlikely precipitation
        Unknown    — value is null or non-numeric

    precip_risk is stored as a separate column in risk_assessments and is
    intentionally excluded from the overall risk_level calculation, which
    remains driven by cargo-specific temperature and humidity thresholds.
    """
    if pd.isna(precip_prob):
        return "Unknown"
    try:
        val = float(precip_prob)
        if val >= PRECIP_RISK_THRESHOLDS["Very High"]:
            return "Very High"
        elif val >= PRECIP_RISK_THRESHOLDS["High"]:
            return "High"
        elif val >= PRECIP_RISK_THRESHOLDS["Moderate"]:
            return "Moderate"
        return "Low"
    except (ValueError, TypeError):
        return "Unknown"


def build_risk_assessments(
    forecast_df:        pd.DataFrame,
    warehouse_cargo_df: pd.DataFrame,
    cargo_df:           pd.DataFrame,
    engine,
) -> pd.DataFrame:
    """
    Generate one risk_assessment row per warehouse × cargo × forecast day,
    but only for forecast dates not already present in risk_assessments
    (incremental strategy — avoids duplicating previously scored days).

    Scores:
        temp_risk     — actual temp_max_f vs cargo temp_max_f threshold
        humidity_risk — actual humidity_avg_pct vs cargo humidity_max_pct

    Overall risk_level  = highest of the two individual risk scores.
    recommended_action  = operational guidance specific to each risk level,
                          drawn from RISK_ACTION_MAP.
    """
    logger.info("Building risk assessments (incremental)...")

    if forecast_df.empty or warehouse_cargo_df.empty or cargo_df.empty:
        logger.warning("Skipping risk assessments — one or more source DataFrames are empty")
        return pd.DataFrame()

    # Filter to only dates that have not already been assessed
    existing_dates = get_existing_assessment_dates(engine)
    new_forecast   = forecast_df[~forecast_df["forecast_date"].isin(existing_dates)].copy()

    if new_forecast.empty:
        logger.info(
            "  — No new assessment dates to process "
            "(all %d forecast date(s) already assessed)", len(forecast_df)
        )
        return pd.DataFrame()

    skipped = len(forecast_df) - len(new_forecast)
    if skipped:
        logger.info("  — Skipping %d date(s) already assessed in risk_assessments", skipped)

    # Fetch forecast_id values assigned by PostgreSQL SERIAL
    with engine.connect() as conn:
        result       = conn.execute(text("SELECT forecast_id, forecast_date FROM daily_forecasts"))
        forecast_map = {row.forecast_date: row.forecast_id for row in result}

    cargo_map = cargo_df.set_index("cargo_type_id").to_dict("index")
    rows      = []

    for _, forecast in new_forecast.iterrows():
        date_key    = forecast["forecast_date"]
        forecast_id = forecast_map.get(date_key)

        if not forecast_id:
            logger.warning("No forecast_id found for date %s — skipping", date_key)
            continue

        temp_max = forecast.get("temp_max_f")
        humidity = forecast.get("humidity_avg_pct")
        precip   = forecast.get("precipitation_prob_pct")

        for _, wc in warehouse_cargo_df.iterrows():
            cargo = cargo_map.get(wc["cargo_type_id"])

            if not cargo:
                logger.warning("cargo_type_id %s not found — skipping", wc["cargo_type_id"])
                continue

            temp_risk     = classify_risk(temp_max, cargo.get("temp_max_f"))
            humidity_risk = classify_risk(humidity, cargo.get("humidity_max_pct"))
            precip_risk   = classify_precip_risk(precip)

            # overall risk_level is driven by cargo-specific thresholds only
            # (temp + humidity). precip_risk is contextual — it uses fixed
            # operational bands with no regulatory equivalent, so including
            # it in overall would inflate scores beyond what standards support.
            priority = {"Unknown": 0, "Low": 1, "Moderate": 2, "High": 3, "Very High": 4}
            overall  = max([temp_risk, humidity_risk], key=lambda r: priority.get(r, 0))

            rows.append({
                "warehouse_id":       wc["warehouse_id"],
                "cargo_type_id":      wc["cargo_type_id"],
                "forecast_id":        forecast_id,
                "forecast_date":      date_key,
                "temp_risk":          temp_risk,
                "humidity_risk":      humidity_risk,
                "precip_risk":        precip_risk,
                "risk_level":         overall,
                "recommended_action": RISK_ACTION_MAP.get(overall, RISK_ACTION_MAP["Unknown"]),
            })

    risk_df = pd.DataFrame(rows)
    logger.info("✓ Generated %d risk assessment row(s) for %d new date(s)",
                len(risk_df), len(new_forecast))
    return risk_df


def write_risk_assessments(risk_df: pd.DataFrame, engine) -> None:
    """Append new risk assessment rows to the database."""
    if risk_df.empty:
        return

    dtype_map = {
        "warehouse_id":  Integer(),
        "cargo_type_id": Integer(),
        "forecast_id":   Integer(),
        "forecast_date": Date(),
        "temp_risk":     String(20),
        "humidity_risk": String(20),
        "precip_risk":   String(20),
        "risk_level":    String(20),
    }

    try:
        risk_df.to_sql(
            "risk_assessments",
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
            dtype=dtype_map,
        )
        logger.info("  ✓ Loaded %d rows into risk_assessments", len(risk_df))
    except Exception as exc:
        logger.error("Failed to load risk_assessments: %s", exc)
        raise


# =============================================================================
# SECTION 10 — ORCHESTRATION
# =============================================================================

def run() -> None:
    """
    Execute the complete ETL pipeline in sequence:

        1. Connect to PostgreSQL
        2. Initialise schema (CREATE IF NOT EXISTS — safe to rerun)
        3. Extract 7-day forecast from Open-Meteo API
        4. Transform & validate forecast data
        5. Save forecast CSV
        6. Load & validate all source CSV files
        7. Upsert seed tables (locations, weather_codes, warehouses,
           cargo_types, warehouse_cargo)
        8. Incrementally insert new forecast rows
        9. Run FK guard for weather codes (V13)
       10. Incrementally build and insert new risk assessments
    """
    logger.info("=" * 60)
    logger.info("COLD-CHAIN LOGISTICS RISK MONITOR — ETL PIPELINE")
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # Step 1 — Connect
    # ------------------------------------------------------------------
    logger.info("[1/10] Connecting to PostgreSQL")
    engine = get_engine()

    # ------------------------------------------------------------------
    # Step 2 — Schema
    # ------------------------------------------------------------------
    logger.info("[2/10] Initialising schema")
    create_schema(engine)

    # ------------------------------------------------------------------
    # Step 3 — Extract
    # ------------------------------------------------------------------
    logger.info("[3/10] Extracting from Open-Meteo API")
    raw_df = extract_weather_forecast()

    # ------------------------------------------------------------------
    # Step 4 — Transform & Validate forecast
    # ------------------------------------------------------------------
    logger.info("[4/10] Transforming and validating forecast")
    forecast_df = transform_forecast(raw_df)
    validate_forecast(forecast_df)

    # ------------------------------------------------------------------
    # Step 5 — Save CSV
    # ------------------------------------------------------------------
    logger.info("[5/10] Saving forecast CSV")
    save_forecast_csv(forecast_df)

    # ------------------------------------------------------------------
    # Step 6 — Load & Validate seed CSVs
    # ------------------------------------------------------------------
    logger.info("[6/10] Loading and validating seed CSV files")
    warehouses, cargo, warehouse_cargo, weather_codes = load_source_files()
    validate_source_data(warehouses, cargo, warehouse_cargo, forecast_df)

    # ------------------------------------------------------------------
    # Step 7 — Upsert seed tables (incremental)
    # ------------------------------------------------------------------
    logger.info("[7/10] Upserting seed tables")
    location_df = build_locations()
    upsert_table(location_df,   "locations",     "location_id",     engine)

    # Query the real location_id back from the DB after upsert.
    # This eliminates any assumption that the inserted value equals 1 —
    # the returned ID is used for every downstream FK so they are always
    # consistent with what is actually stored in the locations table.
    location_id = get_location_id(engine)

    upsert_table(weather_codes, "weather_codes", "weather_code_id", engine)
    upsert_table(
        warehouses.assign(location_id=location_id),   # FK uses queried value
        "warehouses", "warehouse_id", engine,
    )
    upsert_table(cargo,         "cargo_types",   "cargo_type_id",   engine)
    upsert_warehouse_cargo(warehouse_cargo, engine)

    # ------------------------------------------------------------------
    # Step 8 — Incremental forecast insert + FK guard (V13)
    # ------------------------------------------------------------------
    logger.info("[8/10] Inserting new forecast rows (incremental)")
    forecast_load_df = build_forecast_for_load(forecast_df, location_id)  # FK uses queried value
    guard_weather_codes(forecast_load_df, engine)       # V13 before FK insert
    new_count = insert_new_forecasts(forecast_load_df, engine)

    # ------------------------------------------------------------------
    # Step 9 — Risk assessments (incremental)
    # ------------------------------------------------------------------
    logger.info("[9/10] Building and inserting risk assessments (incremental)")
    risk_df = build_risk_assessments(forecast_load_df, warehouse_cargo, cargo, engine)
    write_risk_assessments(risk_df, engine)

    # ------------------------------------------------------------------
    # Step 10 — Summary
    # ------------------------------------------------------------------
    logger.info("[10/10] Pipeline complete")
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("  New forecast rows inserted : %d", new_count)
    logger.info("  Risk assessment rows added : %d", len(risk_df) if not risk_df.empty else 0)
    logger.info("  Forecast CSV               : %s", FORECAST_CSV)
    logger.info("  Next step                  : connect Power BI to PostgreSQL views")
    logger.info("=" * 60)

    logger.info("Preview (first 3 forecast rows):\n%s",
                forecast_df.head(3).to_string(index=False))


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        logger.critical("Pipeline failed: %s", exc)
        sys.exit(1)