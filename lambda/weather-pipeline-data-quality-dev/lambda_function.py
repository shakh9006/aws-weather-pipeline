import os
import json
import logging

import boto3
import awswrangler as wr
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sns_client = boto3.client("sns")
SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "")
ATHENA_OUTPUT = os.environ.get("ATHENA_OUTPUT", "")

MIN_ROW_COUNT = int(os.environ.get("DQ_MIN_ROW_COUNT", "1"))
MAX_NULL_PCT = float(os.environ.get("DQ_MAX_NULL_PCT", "10.0"))
TEMP_MIN_PLAUSIBLE = -90.0
TEMP_MAX_PLAUSIBLE = 60.0

CRITICAL_COLUMNS = ["city", "temp", "observed_at", "country"]

def run_query(sql: str, database: str) -> pd.DataFrame:
    return wr.athena.read_sql_query(
        sql=sql,
        database=database,
        ctas_approach=False,
        s3_output=ATHENA_OUTPUT if ATHENA_OUTPUT else None,
    )

def lambda_handler(event, context):
    database = event.get("database", "weather_pipeline_silver_dev")
    table = event.get("table", "clean_observations")

    checks = []
    overall = True

    def add(name, passed, message, **extra):
        nonlocal overall
        rec = {"check": name, "passed": bool(passed), "message": message}
        rec.update(extra)
        checks.append(rec)

        if not passed:
            overall = False
        logger.info(f"{name}: {'PASS' if passed else 'FAIL'} - {message}")

    try:
        df = run_query(f'SELECT * FROM {table} LIMIT 10000', database)
    except Exception as e:
        add("read_table", False, f"Could not read {database}.{table}: {e}")
        _maybe_alert(checks, overall)
        return _result(overall, checks)

    add("row_count", len(df) >= MIN_ROW_COUNT, f"rows={len(df)} (min {MIN_ROWS_COUNT})", value=len(df))

    missing = [c for c in CRITICAL_COLUMNS if c not in df.columns]
    add("schema", len(missing) == 0, f"missing columns: {missing}" if missing else "all critical columns present", missing=missing)

    if len(df) > 0:
        for c in CRITICAL_COLUMNS:
            if c in df.columns:
                null_pct = df[c].isna().sum() / len(df) * 100
                add(f"null_pct[{c}]", null_pct <= MAX_NULL_PCT,
                    f"{c} null%={null_pct:.2f} (max {MAX_NULL_PCT})",
                    value=round(null_pct, 2))

    if "temp" in df.columns and len(df) > 0:
        bad = df[(df["temp"] < TEMP_MIN_PLAUSIBLE) | (df["temp"] > TEMP_MAX_PLAUSIBLE)]
        add("temp_range", len(bad) == 0,
            f"{len(bad)} rows with implausible temp", value=int(len(bad)))

    _maybe_alert(checks, overall)
    return _result(overall, checks)


def _maybe_alert(checks, overall):
    if not overall and SNS_TOPIC:
        failed = [c for c in checks if not c["passed"]]
        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Subject="[Weather Pipeline] Data quality FAILED",
            Message=json.dumps(failed, indent=2, default=str),
        )

def _result(overall, checks):
    passed = sum(1 for c in checks if c["passed"])
    return {
        "quality_passed": bool(overall),
        "checks_passed": passed,
        "checks_total": len(checks),
        "details": json.loads(json.dumps(checks, default=str)),
    }
