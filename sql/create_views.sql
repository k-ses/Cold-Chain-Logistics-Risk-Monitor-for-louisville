-- Cold-Chain Logistics Risk Monitor
-- Power BI Views
-- Run once after the first successful pipeline run.
-- Re-run any time the schema changes.

CREATE OR REPLACE VIEW vw_daily_forecast AS
SELECT
    df.forecast_id,
    df.forecast_date,
    df.temp_max_f,
    df.temp_min_f,
    df.humidity_avg_pct,
    df.precipitation_prob_pct,
    wc.description        AS weather_description,
    wc.icon               AS weather_icon,
    wc.category           AS weather_category,
    wc.severity_level     AS weather_severity,
    wc.color_hex
FROM daily_forecasts df
JOIN weather_codes wc ON df.weather_code_id = wc.weather_code_id;


CREATE OR REPLACE VIEW vw_risk_assessments AS
SELECT
    ra.risk_id,
    ra.forecast_date,
    w.warehouse_name,
    w.street_address,
    ct.cargo_name,
    ct.regulatory_body,
    ct.temp_max_f         AS cargo_temp_threshold,
    ct.humidity_max_pct   AS cargo_humidity_threshold,
    df.temp_max_f         AS actual_temp,
    df.humidity_avg_pct   AS actual_humidity,
    df.precipitation_prob_pct AS actual_precip_prob,
    ra.temp_risk,
    ra.humidity_risk,
    ra.precip_risk,
    ra.risk_level,
    ra.recommended_action,
    wc.description        AS weather_description,
    wc.icon               AS weather_icon,
    wc.category           AS weather_category
FROM risk_assessments ra
JOIN warehouses      w   ON ra.warehouse_id   = w.warehouse_id
JOIN cargo_types     ct  ON ra.cargo_type_id  = ct.cargo_type_id
JOIN daily_forecasts df  ON ra.forecast_id    = df.forecast_id
JOIN weather_codes   wc  ON df.weather_code_id = wc.weather_code_id;


CREATE OR REPLACE VIEW vw_warehouse_summary AS
SELECT
    ra.forecast_date,
    w.warehouse_name,
    COUNT(DISTINCT ra.cargo_type_id)                              AS cargo_types_stored,
    SUM(CASE WHEN ra.risk_level = 'Very High' THEN 1 ELSE 0 END) AS very_high_count,
    SUM(CASE WHEN ra.risk_level = 'High'      THEN 1 ELSE 0 END) AS high_count,
    SUM(CASE WHEN ra.risk_level = 'Moderate'  THEN 1 ELSE 0 END) AS moderate_count,
    SUM(CASE WHEN ra.risk_level = 'Low'       THEN 1 ELSE 0 END) AS low_count,
    MAX(CASE
        WHEN ra.risk_level = 'Very High' THEN 4
        WHEN ra.risk_level = 'High'      THEN 3
        WHEN ra.risk_level = 'Moderate'  THEN 2
        WHEN ra.risk_level = 'Low'       THEN 1
        ELSE 0 END)                                               AS max_risk_score,
    MAX(ra.risk_level)                                            AS worst_risk_level
FROM risk_assessments ra
JOIN warehouses w ON ra.warehouse_id = w.warehouse_id
GROUP BY ra.forecast_date, w.warehouse_name;


CREATE OR REPLACE VIEW vw_executive_kpis AS
SELECT
    df.forecast_date,
    df.temp_max_f,
    df.temp_min_f,
    df.humidity_avg_pct,
    df.precipitation_prob_pct,
    df.weather_description,
    df.weather_icon,
    COUNT(DISTINCT ra.risk_id)                                    AS total_assessments,
    SUM(CASE WHEN ra.risk_level = 'Very High' THEN 1 ELSE 0 END) AS very_high_count,
    SUM(CASE WHEN ra.risk_level = 'High'      THEN 1 ELSE 0 END) AS high_count,
    SUM(CASE WHEN ra.risk_level = 'Moderate'  THEN 1 ELSE 0 END) AS moderate_count,
    SUM(CASE WHEN ra.risk_level = 'Low'       THEN 1 ELSE 0 END) AS low_count,
    MAX(CASE
        WHEN ra.risk_level = 'Very High' THEN 4
        WHEN ra.risk_level = 'High'      THEN 3
        WHEN ra.risk_level = 'Moderate'  THEN 2
        WHEN ra.risk_level = 'Low'       THEN 1
        ELSE 0 END)                                               AS max_risk_score
FROM vw_daily_forecast df
LEFT JOIN risk_assessments ra ON df.forecast_id = ra.forecast_id
GROUP BY df.forecast_date, df.temp_max_f, df.temp_min_f,
         df.humidity_avg_pct, df.precipitation_prob_pct,
         df.weather_description, df.weather_icon;