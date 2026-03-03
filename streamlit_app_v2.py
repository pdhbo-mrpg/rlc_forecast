"""Snowflake-native Streamlit app v2 for RLC Prophet forecasting pipeline."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import streamlit as st
from prophet import Prophet
from snowflake.snowpark.context import get_active_session

try:
    from hierarchicalforecast.core import HierarchicalReconciliation
    from hierarchicalforecast.methods import MinTrace
    from hierarchicalforecast.utils import aggregate
except ImportError as exc:
    raise SystemExit("hierarchicalforecast is required. Install dependencies.") from exc

ROOT = Path(__file__).resolve().parent
ARCHIVE_SQL = ROOT / "archive.sql"
REFORMAT_SQL = ROOT / "Reformat_history.sql"
AGG_SQL = ROOT / "Agg_forecast.sql"

# config.py based settings
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "DATA_VAULT_DEV")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "DBT_MBOTHA")
SOURCE_TABLE = os.getenv("SOURCE_TABLE", "RLC_GTS_HIST_WEEK")
TARGET_TABLE = os.getenv("TARGET_TABLE", "DATA_VAULT_DEV.DBT_MBOTHA.RLC_GTS_FORECAST")
FORECAST_HORIZON = int(os.getenv("FORECAST_HORIZON", "52"))

HISTORY_TABLE = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SOURCE_TABLE}"
FORECAST_TABLE = TARGET_TABLE
ARCHIVE_TABLE = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.RLC_FCST_MTH_ARCHIVE"
LIVE_MONTHLY_TABLE = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.RLC_FCST_MTH"

DATE_COL = "calendardate"
VALUE_COL = "sales"
LEVEL_COLS = [
    "entity_l1",
    "entity_l2",
    "entity_l3",
    "prod_l1",
    "prod_l2",
    "prod_l3",
    "prod_l4",
    "region_l1",
    "region_l2",
    "region_l3",
]


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
            outputs.append({"statement": cleaned[:160], "rows": [row.as_dict() for row in result]})

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
    if not table_exists(session, ARCHIVE_TABLE):
        st.warning(f"Archive table not found. Creating `{ARCHIVE_TABLE}` from current monthly forecast structure.")
        session.sql(f"CREATE TABLE {ARCHIVE_TABLE} LIKE {LIVE_MONTHLY_TABLE}").collect()

    archive_outputs = run_sql_file(session, ARCHIVE_SQL)
    if archive_outputs:
        st.json(archive_outputs)


def build_hierarchy(df: pd.DataFrame):
    """Aggregate hierarchy using hierarchicalforecast.utils.aggregate."""
    df_hier = df.copy()
    for col in LEVEL_COLS:
        df_hier[col] = f"{col}__" + df_hier[col].astype(str)

    spec: List[List[str]] = [
        ["entity_l1"],
        ["entity_l1", "entity_l2"],
        ["entity_l1", "entity_l2", "entity_l3"],
        ["prod_l1"],
        ["prod_l1", "prod_l2"],
        ["prod_l1", "prod_l2", "prod_l3"],
        ["prod_l1", "prod_l2", "prod_l3", "prod_l4"],
        ["region_l1"],
        ["region_l1", "region_l2"],
        ["region_l1", "region_l2", "region_l3"],
        LEVEL_COLS,
    ]

    Y_df, S, tags = aggregate(df=df_hier, spec=spec)
    return Y_df, S, tags


def get_retail_holidays() -> pd.DataFrame:
    """Create Black Friday holiday rows shifted to Sunday retail week."""
    bf_dates_actual = [
        "2018-11-23",
        "2019-11-29",
        "2020-11-27",
        "2021-11-26",
        "2022-11-25",
        "2023-11-24",
        "2024-11-29",
        "2025-11-28",
        "2026-11-27",
        "2027-11-26",
        "2028-11-24",
        "2029-11-23",
    ]
    bf_dates_sunday = pd.to_datetime(bf_dates_actual) - pd.Timedelta(days=5)
    return pd.DataFrame({"holiday": "BlackFriday", "ds": bf_dates_sunday, "lower_window": 0, "upper_window": 1})


def run_prophet_on_hierarchy(Y_df: pd.DataFrame, horizon: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Fit Prophet to each hierarchy node and produce future + in-sample forecasts."""
    future_forecasts_list = []
    insample_fitted_list = []

    if "unique_id" in Y_df.columns:
        Y_df = Y_df.set_index("unique_id")

    retail_holidays = get_retail_holidays()

    for uid in Y_df.index.unique():
        series_data = Y_df.loc[[uid]].reset_index().sort_values("ds").dropna(subset=["y"])
        if len(series_data) < 5:
            continue

        model = Prophet(
            seasonality_mode="multiplicative",
            yearly_seasonality=False,
            weekly_seasonality=False,
            daily_seasonality=False,
            changepoint_prior_scale=0.05,
            seasonality_prior_scale=10.0,
            holidays=retail_holidays,
        )
        model.add_seasonality(name="yearly", period=365.25, fourier_order=12)
        model.add_country_holidays(country_name="ZA")

        model.fit(series_data)
        future = model.make_future_dataframe(periods=horizon, freq="W-SUN")
        forecast = model.predict(future)

        fcst_future = forecast.iloc[-horizon:].copy()
        future_forecasts_list.append(
            pd.DataFrame({"ds": fcst_future["ds"], "unique_id": uid, "Prophet": fcst_future["yhat"]})
        )

        fcst_history = forecast.iloc[:-horizon].copy()
        insample = series_data[["ds", "y"]].copy()
        insample["unique_id"] = uid
        insample["Prophet"] = fcst_history["yhat"].values
        insample_fitted_list.append(insample)

    if not future_forecasts_list or not insample_fitted_list:
        raise ValueError("No series had sufficient data for Prophet.")

    return pd.concat(future_forecasts_list).reset_index(drop=True), pd.concat(insample_fitted_list).reset_index(drop=True)


def forecast(df: pd.DataFrame, horizon: int) -> pd.DataFrame:
    """Run hierarchical Prophet and MinTrace reconciliation."""
    df = df.copy().rename(columns={DATE_COL: "ds", VALUE_COL: "y"})
    df = df[LEVEL_COLS + ["ds", "y"]]

    Y_df, S, tags = build_hierarchy(df)
    Y_hat_df, Y_fitted_df = run_prophet_on_hierarchy(Y_df, horizon)

    common_ids = set(Y_hat_df["unique_id"]) & set(Y_fitted_df["unique_id"])
    Y_hat_df = Y_hat_df[Y_hat_df["unique_id"].isin(common_ids)]
    Y_fitted_df = Y_fitted_df[Y_fitted_df["unique_id"].isin(common_ids)]

    hrec = HierarchicalReconciliation(reconcilers=[MinTrace(method="mint_shrink", nonnegative=False)])
    reconciled = hrec.reconcile(Y_hat_df=Y_hat_df, Y_df=Y_fitted_df, S_df=S, tags=tags).reset_index()

    bottom_level = reconciled[reconciled["unique_id"].isin(S.columns)].copy()
    model_col_name = "Prophet/MinTrace_method_mint_shrink"
    if model_col_name not in bottom_level.columns:
        model_col_name = bottom_level.columns[-1]

    bottom_level["yhat"] = bottom_level[model_col_name].clip(lower=0)
    expanded = bottom_level["unique_id"].str.split("/", expand=True)
    expanded.columns = LEVEL_COLS

    for col in LEVEL_COLS:
        expanded[col] = expanded[col].str.replace(f"{col}__", "", regex=False)
        expanded[col] = expanded[col].str.replace("|", "/", regex=False)

    bottom_level = pd.concat([bottom_level, expanded], axis=1)
    bottom_level["retail_week"] = pd.to_datetime(bottom_level["ds"])
    return bottom_level[LEVEL_COLS + ["retail_week", "yhat"]]


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
        table_name=FORECAST_TABLE.split(".")[-1],
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        auto_create_table=True,
        overwrite=True,
    )
    return len(out_pdf)


def main() -> None:
    st.set_page_config(page_title="RLC Prophet Forecast Pipeline V2", layout="wide")
    st.title("RLC Forecast Pipeline v2 (Snowflake Native Streamlit)")
    st.caption("Embedded config + forecast logic (no imports from config.py or forecast_pipeline_prophet.py)")

    session = get_active_session()
    horizon = st.number_input(
        "Forecast horizon (weeks)", min_value=1, max_value=104, value=FORECAST_HORIZON, step=1
    )

    if st.button("Run pipeline v2", type="primary"):
        with st.status("Running pipeline v2...", expanded=True) as status:
            st.write("Step 1/4: Archive existing forecasts (if needed)")
            ensure_archive_and_run(session)

            st.write("Step 2/4: Reformat history for forecasting model")
            run_sql_file(session, REFORMAT_SQL)

            st.write("Step 3/4: Run Prophet + hierarchical reconciliation")
            row_count = run_prophet_forecast(session, int(horizon))
            st.success(f"Weekly forecast generated with {row_count:,} rows in {FORECAST_TABLE}.")

            st.write("Step 4/4: Aggregate weekly forecasts to monthly tables")
            run_sql_file(session, AGG_SQL)

            status.update(label="Pipeline v2 complete", state="complete")


if __name__ == "__main__":
    main()
