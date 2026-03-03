"""Snowflake-native Streamlit app for RLC Prophet forecasting pipeline."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import streamlit as st
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.context import get_active_session

from forecast_pipeline_prophet import forecast

ROOT = Path(__file__).resolve().parent
ARCHIVE_SQL = ROOT / "archive.sql"
REFORMAT_SQL = ROOT / "Reformat_history.sql"
AGG_SQL = ROOT / "Agg_forecast.sql"

HISTORY_TABLE = "DATA_VAULT_DEV.DBT_MBOTHA.RLC_GTS_HIST_WEEK"
FORECAST_TABLE = "DATA_VAULT_DEV.DBT_MBOTHA.RLC_GTS_FORECAST"
ARCHIVE_TABLE = "DATA_VAULT_DEV.DBT_MBOTHA.RLC_FCST_MTH_ARCHIVE"
LIVE_MONTHLY_TABLE = "DATA_VAULT_DEV.DBT_MBOTHA.RLC_FCST_MTH"


def split_sql_statements(sql_text: str) -> list[str]:
    """Split SQL script into statements while preserving $$ scripting blocks."""
    statements: list[str] = []
    current: list[str] = []
    in_dollar_block = False

    i = 0
    while i < len(sql_text):
        if sql_text[i : i + 2] == "$$":
            in_dollar_block = not in_dollar_block
            current.append("$$")
            i += 2
            continue

        ch = sql_text[i]
        if ch == ";" and not in_dollar_block:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
        else:
            current.append(ch)
        i += 1

    trailing = "".join(current).strip()
    if trailing:
        statements.append(trailing)

    return statements


def run_sql_file(session, file_path: Path) -> list[dict]:
    """Execute each SQL statement in file and return any row-level outputs."""
    sql_text = file_path.read_text(encoding="utf-8")
    statements = split_sql_statements(sql_text)
    outputs: list[dict] = []

    for idx, statement in enumerate(statements, start=1):
        cleaned = re.sub(r"\s+", " ", statement).strip()
        st.write(f"Executing statement {idx}/{len(statements)} from `{file_path.name}`")
        result = session.sql(statement).collect()
        if result:
            outputs.append(
                {
                    "statement": cleaned[:160],
                    "rows": [row.as_dict() for row in result],
                }
            )

    return outputs


def table_exists(session, fully_qualified_name: str) -> bool:
    """Check whether a table exists in Snowflake."""
    db, schema, table = fully_qualified_name.split(".")
    rows = (
        session.sql(
            f"""
            SELECT 1
            FROM {db}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema.upper()}'
              AND TABLE_NAME = '{table.upper()}'
            LIMIT 1
            """
        )
        .collect()
    )
    return bool(rows)


def ensure_archive_and_run(session) -> None:
    """Create archive table if needed and run archive logic."""
    try:
        if not table_exists(session, ARCHIVE_TABLE):
            st.warning(f"Archive table not found. Creating `{ARCHIVE_TABLE}` from current monthly forecast structure.")
            session.sql(f"CREATE TABLE {ARCHIVE_TABLE} LIKE {LIVE_MONTHLY_TABLE}").collect()

        archive_outputs = run_sql_file(session, ARCHIVE_SQL)
        if archive_outputs:
            st.json(archive_outputs)
    except SnowparkSQLException as exc:
        err = str(exc)
        if "42501" in err or "Insufficient privileges" in err:
            st.warning(
                "Skipping archive step due to insufficient privileges on "
                f"`{ARCHIVE_TABLE}`. Grant SELECT/INSERT on the archive table to enable this step."
            )
            return
        raise


def run_prophet_forecast(session, horizon: int) -> int:
    """Load history, run hierarchical Prophet forecast, and write weekly results."""
    history_pdf = session.table(HISTORY_TABLE).to_pandas()
    history_pdf.columns = [c.lower() for c in history_pdf.columns]

    forecast_pdf = forecast(history_pdf, horizon=horizon)
    out_pdf = forecast_pdf[["entity_l1", "prod_l1", "region_l1", "retail_week", "yhat"]].copy()
    out_pdf = out_pdf.rename(columns={"yhat": "FCST"})
    out_pdf.columns = [c.upper() for c in out_pdf.columns]

    out_pdf["PROD_L1"] = pd.to_numeric(out_pdf["PROD_L1"], errors="coerce").fillna(0).astype(int)
    out_pdf["REGION_L1"] = pd.to_numeric(out_pdf["REGION_L1"], errors="coerce").fillna(0).astype(int)
    out_pdf["RETAIL_WEEK"] = pd.to_datetime(out_pdf["RETAIL_WEEK"]).dt.date
    out_pdf["FCST"] = out_pdf["FCST"].astype(float)

    session.sql(f"DROP TABLE IF EXISTS {FORECAST_TABLE}").collect()
    session.write_pandas(
        df=out_pdf,
        table_name="RLC_GTS_FORECAST",
        database="DATA_VAULT_DEV",
        schema="DBT_MBOTHA",
        auto_create_table=True,
        overwrite=True,
    )
    return len(out_pdf)


def main() -> None:
    st.set_page_config(page_title="RLC Prophet Forecast Pipeline", layout="wide")
    st.title("RLC Forecast Pipeline (Snowflake Native Streamlit)")
    st.caption("Workflow: archive → reformat history → prophet + MinTrace reconciliation → monthly aggregation")

    session = get_active_session()

    horizon = st.number_input("Forecast horizon (weeks)", min_value=1, max_value=104, value=52, step=1)

    if st.button("Run pipeline", type="primary"):
        with st.status("Running pipeline...", expanded=True) as status:
            st.write("Step 1/4: Archive existing forecasts (if needed)")
            ensure_archive_and_run(session)

            st.write("Step 2/4: Reformat history for forecasting model")
            run_sql_file(session, REFORMAT_SQL)

            st.write("Step 3/4: Run Prophet + hierarchical reconciliation")
            row_count = run_prophet_forecast(session, int(horizon))
            st.success(f"Weekly forecast generated with {row_count:,} rows in {FORECAST_TABLE}.")

            st.write("Step 4/4: Aggregate weekly forecasts to monthly tables")
            run_sql_file(session, AGG_SQL)

            status.update(label="Pipeline complete", state="complete")


if __name__ == "__main__":
    main()
