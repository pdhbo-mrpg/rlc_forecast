"""
End-to-end retail sales forecasting pipeline using Facebook Prophet.

Steps:
1. Read historic weekly retail sales from Snowflake (using CALENDARDATE).
2. Clean history (missing values + outlier clipping per series).
3. Aggregate data to create the hierarchy (handling ID collisions via prefixing).
4. Fit Prophet models on ALL levels with RETAIL-SPECIFIC HYPERPARAMETERS.
5. Reconcile with MinTrace (Non-negative constraint handled via post-processing clipping).
6. Clean output and write to Snowflake.
"""

from __future__ import annotations

import logging
import time
import os
from dataclasses import dataclass
from typing import Callable, Dict, List, Tuple
import certifi

import numpy as np
import pandas as pd
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from snowflake.sqlalchemy import URL

# Replaces StatsForecast/AutoARIMA
from prophet import Prophet

# Native Snowflake Writer
from snowflake.connector.pandas_tools import write_pandas

import config

# HierarchicalForecast imports
try:
    from hierarchicalforecast.core import HierarchicalReconciliation
    from hierarchicalforecast.methods import BottomUp, MinTrace, ERM
    from hierarchicalforecast.utils import aggregate
except ImportError as exc:
    raise SystemExit(
        "hierarchicalforecast is required. Install dependencies."
    ) from exc

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger(__name__)

# --- CONFIGURATION ---
DATE_COL = "calendardate"
VALUE_COL = "sales"

# Updated Hierarchy Levels
LEVEL_COLS = [
    "entity_l1",
    "entity_l2",
    "entity_l3", # NEW
    "prod_l1",
    "prod_l2",
    "prod_l3",
    "prod_l4",   # NEW
    "region_l1",
    "region_l2",
    "region_l3", # NEW
]


@dataclass
class SnowflakeConfig:
    account: str
    user: str
    password: str
    private_key_path: str
    private_key_passphrase: str
    token: str
    authenticator: str
    role: str
    warehouse: str
    database: str
    schema: str
    source_table: str
    target_table: str

    @classmethod
    def from_config(cls) -> "SnowflakeConfig":
        return cls(
            account=config.SNOWFLAKE_ACCOUNT,
            user=config.SNOWFLAKE_USER,
            password=config.SNOWFLAKE_PASSWORD,
            private_key_path=getattr(config, "SNOWFLAKE_PRIVATE_KEY_PATH", ""),
            private_key_passphrase=getattr(config, "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", ""),
            token=config.SNOWFLAKE_TOKEN,
            authenticator=config.SNOWFLAKE_AUTHENTICATOR,
            role=config.SNOWFLAKE_ROLE,
            warehouse=config.SNOWFLAKE_WAREHOUSE,
            database=config.SNOWFLAKE_DATABASE,
            schema=config.SNOWFLAKE_SCHEMA,
            source_table=config.SOURCE_TABLE,
            target_table=config.TARGET_TABLE,
        )


def snowflake_engine(sf_conf: SnowflakeConfig) -> Engine:
    """Build a SQLAlchemy engine for Snowflake."""
    connect_args = {}
    authenticator = sf_conf.authenticator
    password = sf_conf.password

    def load_private_key_der(path: str, passphrase: str):
        with open(path, "rb") as fh:
            key_data = fh.read()
        pwd = passphrase.encode() if passphrase else None
        try:
            key = serialization.load_pem_private_key(key_data, password=pwd, backend=default_backend())
        except ValueError:
            key = serialization.load_der_private_key(key_data, password=pwd, backend=default_backend())
        return key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    def normalize_account(account: str) -> str:
        acct = account.strip()
        if acct.startswith("https://"):
            acct = acct[len("https://"):]
        if acct.startswith("http://"):
            acct = acct[len("http://"):]
        if ".snowflakecomputing.com" in acct:
            acct = acct.split(".snowflakecomputing.com")[0]
        return acct.split("/")[0].split(":")[0]

    if sf_conf.private_key_path:
        connect_args["private_key"] = load_private_key_der(
            sf_conf.private_key_path, sf_conf.private_key_passphrase
        )
        authenticator = "snowflake"
        password = ""
    elif sf_conf.token:
        authenticator = "oauth"
        connect_args["token"] = sf_conf.token
        password = ""

    url = URL(
        account=normalize_account(sf_conf.account),
        user=sf_conf.user,
        password=password,
        database=sf_conf.database,
        schema=sf_conf.schema,
        warehouse=sf_conf.warehouse,
        role=sf_conf.role,
        authenticator=authenticator,
    )
    if getattr(config, "SNOWFLAKE_INSECURE_MODE", False):
        connect_args["insecure_mode"] = True

    def build_ca_bundle() -> str | None:
        """
        Create a combined CA bundle that includes the system certs (certifi)
        plus any corporate bundle provided. This helps both Snowflake and
        Azure blob downloads behind TLS inspection.
        """
        paths: List[str] = []
        # Always include certifi to keep public roots.
        paths.append(certifi.where())

        extra = getattr(config, "SNOWFLAKE_CA_BUNDLE", "")
        if extra and os.path.isfile(extra):
            paths.append(extra)
        elif extra:
            LOGGER.warning("Configured CA bundle not found: %s", extra)

        # If only certifi is present, just return it directly.
        if len(paths) == 1:
            return paths[0]

        combined_path = os.path.join(os.path.dirname(__file__), "combined_ca_bundle.pem")
        try:
            with open(combined_path, "w", encoding="utf-8") as out_f:
                for p in paths:
                    with open(p, "r", encoding="utf-8") as in_f:
                        out_f.write(in_f.read())
                        if not out_f.tell() or out_f.tell() == 0:
                            out_f.write("\n")
            LOGGER.info("Using combined CA bundle: %s", combined_path)
            return combined_path
        except Exception as exc:  # pragma: no cover
            LOGGER.warning("Failed to build combined CA bundle (%s). Falling back to certifi only.", exc)
            return paths[0]

    ca_bundle = build_ca_bundle()
    if ca_bundle:
        os.environ.setdefault("REQUESTS_CA_BUNDLE", ca_bundle)
        os.environ.setdefault("SNOWFLAKE_CA_BUNDLE", ca_bundle)
        connect_args["session_parameters"] = connect_args.get("session_parameters", {})
        LOGGER.info("CA bundle in use: %s", ca_bundle)
    return create_engine(url, connect_args=connect_args)


def load_history(engine: Engine, source_table: str) -> pd.DataFrame:
    """Load historic retail sales data from Snowflake."""
    cols = ", ".join([DATE_COL] + LEVEL_COLS + [VALUE_COL])
    query = f"SELECT {cols} FROM {source_table}"
    LOGGER.info("Loading history from %s", source_table)
    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows = result.fetchall()
        df = pd.DataFrame(rows, columns=result.keys())
    
    df.columns = [str(c).strip().lower() for c in df.columns]
    LOGGER.info("Sample of loaded data:\n%s", df.head(5))
    return df


# def clean_history(df: pd.DataFrame) -> pd.DataFrame:
#     """Clean history: parse dates, impute missing, clip outliers, sanitize columns."""
#     df = df.copy()
#     LOGGER.info("Parsing retail weeks and cleaning missing/outliers")
    
#     # 1. Parse Calendar Date
#     df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")
    
#     # 2. Sanitize IDs
#     # Add new integer-based levels to the cleaning list
#     int_cols = ['prod_l1', 'prod_l4', 'region_l1', 'region_l3']
#     for col in int_cols:
#         if col in df.columns:
#             df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int).astype(str)

#     # 3. Replace '/' with '|' in data values
#     for col in LEVEL_COLS:
#         if df[col].dtype == object or df[col].dtype.name == 'category':
#             df[col] = df[col].astype(str).str.replace("/", "|")

#     df = df.sort_values(LEVEL_COLS + [DATE_COL])

#     # 4. Outlier Handling
#     # def _impute_and_clip(series: pd.Series) -> pd.Series:
#     #     filled = series.ffill().bfill()
#     #     if filled.isna().all():
#     #         filled = filled.fillna(0)
#     #     mean_val = filled.mean()
#     #     filled = filled.fillna(mean_val)
#     #     q1, q3 = filled.quantile([0.05, 0.95]) 
#     #     iqr = q3 - q1
#     #     lower = max(0.0, q1 - 1.5 * iqr)
#     #     upper = q3 + 2.0 * iqr 
#     #     return filled.clip(lower=lower, upper=upper)

#     # df[VALUE_COL] = df.groupby(LEVEL_COLS, observed=True)[VALUE_COL].transform(_impute_and_clip)
#     LOGGER.info("Finished cleaning history")
#     return df[LEVEL_COLS + [DATE_COL, VALUE_COL]]


def build_hierarchy(df: pd.DataFrame):
    """Aggregate hierarchy using hierarchicalforecast.utils.aggregate."""
    
    # Prefix IDs to prevent collision
    df_hier = df.copy()
    for col in LEVEL_COLS:
        df_hier[col] = f"{col}__" + df_hier[col].astype(str)

    df_hier = df_hier.loc[:, ~df_hier.columns.duplicated()]
    
    # Update spec with new levels
    spec: List[List[str]] = [
        ["entity_l1"],
        ["entity_l1", "entity_l2"],
        ["entity_l1", "entity_l2", "entity_l3"], # NEW
        ["prod_l1"],
        ["prod_l1", "prod_l2"],
        ["prod_l1", "prod_l2", "prod_l3"],
        ["prod_l1", "prod_l2", "prod_l3", "prod_l4"], # NEW
        ["region_l1"],
        ["region_l1", "region_l2"],
        ["region_l1", "region_l2", "region_l3"], # NEW
        LEVEL_COLS,  # bottom level
    ]
    LOGGER.info("Aggregating hierarchy for reconciliation")
    
    Y_df, S, tags = aggregate(df=df_hier, spec=spec)
    Y_df = Y_df.loc[:, ~Y_df.columns.duplicated()]
    return Y_df, S, tags


# --- NEW HELPER FUNCTION FOR HOLIDAYS ---
def get_retail_holidays() -> pd.DataFrame:
    """
    Creates a DataFrame of Black Friday dates shifted to the Sunday start-of-week.
    """
    # Actual Black Friday Dates (Fridays)
    bf_dates_actual = [
        "2018-11-23", "2019-11-29", "2020-11-27", "2021-11-26",
        "2022-11-25", "2023-11-24", "2024-11-29", "2025-11-28",
        "2026-11-27", "2027-11-26", "2028-11-24", "2029-11-23"
    ]
    
    # Shift Back 5 Days (Friday -> Previous Sunday)
    bf_dates_sunday = pd.to_datetime(bf_dates_actual) - pd.Timedelta(days=5)
    
    holidays = pd.DataFrame({
        'holiday': 'BlackFriday',
        'ds': bf_dates_sunday,
        'lower_window': 0,
        'upper_window': 1 
    })
    
    return holidays


def run_prophet_on_hierarchy(
    Y_df: pd.DataFrame,
    horizon: int,
    progress_callback: Callable[[int, int], None] | None = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Fits Prophet models optimized for Retail Weekly Data with Custom Holidays."""
    future_forecasts_list = []
    insample_fitted_list = []
    
    if "unique_id" in Y_df.columns:
        Y_df = Y_df.set_index("unique_id")

    unique_ids = Y_df.index.unique()
    total_series = len(unique_ids)
    LOGGER.info(f"Fitting Prophet models for {total_series} series...")
    
    # Generate Custom Holiday DataFrame
    retail_holidays = get_retail_holidays()
    
    skipped = 0

    for i, uid in enumerate(unique_ids):
        if progress_callback:
            progress_callback(i, total_series)
        if i % 100 == 0:
            LOGGER.info(f"Processing series [{i+1}/{total_series}]")

        series_data = (
            Y_df.loc[[uid]]
            .reset_index()
            .sort_values("ds")
            .dropna(subset=["y"])
        )

        if len(series_data) < 5: 
            skipped += 1
            if skipped <= 10:
                LOGGER.warning("Skipping series %s: not enough data (%d rows)", uid, len(series_data))
            continue
            
        m = Prophet(
            seasonality_mode='multiplicative', 
            yearly_seasonality=False, 
            weekly_seasonality=False, 
            daily_seasonality=False,
            # REDUCED from 0.1 to 0.05 to prevent trend from chasing noise
            changepoint_prior_scale=0.05, 
            # NEW: Constrains how "tall" the seasonal waves can get. 
            # Lower values = stiffer seasonality (prevents wild swings)
            seasonality_prior_scale=10.0, 
            holidays=retail_holidays 
        )

        m.add_seasonality(name='yearly', period=365.25, fourier_order=12)
        m.add_country_holidays(country_name='ZA')

        logging.getLogger("cmdstanpy").setLevel(logging.WARNING)

        try:
            m.fit(series_data)

            future = m.make_future_dataframe(periods=horizon, freq="W-SUN")
            forecast = m.predict(future)

            # Future Part
            fcst_future = forecast.iloc[-horizon:].copy()
            fcst_output = pd.DataFrame(
                {
                    "ds": fcst_future["ds"],
                    "unique_id": uid,
                    "Prophet": fcst_future["yhat"],
                }
            )
            
            # In-Sample Part
            fcst_history = forecast.iloc[:-horizon].copy()
            insample = series_data[["ds", "y"]].copy()
            insample["unique_id"] = uid
            insample["Prophet"] = fcst_history["yhat"].values
            
            future_forecasts_list.append(fcst_output)
            insample_fitted_list.append(insample)

        except Exception as e:
            LOGGER.error(f"Failed to fit Prophet for {uid}: {e}")
            skipped += 1

    if not future_forecasts_list or not insample_fitted_list:
        raise ValueError("No series had sufficient data for Prophet.")

    Y_hat_df = pd.concat(future_forecasts_list).reset_index(drop=True)
    Y_fitted_df = pd.concat(insample_fitted_list).reset_index(drop=True)
    
    LOGGER.info(f"Successfully processed {total_series - skipped} series (Skipped {skipped})")
    if progress_callback:
        progress_callback(total_series, total_series)
        
    return Y_hat_df, Y_fitted_df


def forecast(
    df: pd.DataFrame,
    horizon: int,
    progress_callback: Callable[[int, int], None] | None = None,
) -> pd.DataFrame:
    """
    Fit Prophet models and reconcile using MinTrace (mint_shrink).
    """
    df = df.copy()
    df = df.loc[:, ~df.columns.duplicated()]
    df = df.rename(columns={DATE_COL: "ds", VALUE_COL: "y"})
    
    if "unique_id" in df.columns:
        df = df.drop(columns=["unique_id"])

    df = df[LEVEL_COLS + ["ds", "y"]]

    # 1. Build Hierarchy
    Y_df, S, tags = build_hierarchy(df)

    # 2. Run Prophet Base Models
    LOGGER.info("Fitting base Prophet models and generating in-sample residuals...")
    Y_hat_df, Y_fitted_df = run_prophet_on_hierarchy(
        Y_df,
        horizon,
        progress_callback=progress_callback,
    )

    # Intersection Check
    common_ids = set(Y_hat_df['unique_id']) & set(Y_fitted_df['unique_id'])
    Y_hat_df = Y_hat_df[Y_hat_df['unique_id'].isin(common_ids)]
    Y_fitted_df = Y_fitted_df[Y_fitted_df['unique_id'].isin(common_ids)]

    # 3. Reconciliation
    LOGGER.info("Reconciling with MinTrace (mint_shrink)...")
    
    # nonnegative=False to avoid LinAlgError (we clip manually)
    reconcilers = [BottomUp(),
        MinTrace(method="mint_shrink", nonnegative=False),
    ]
    
    hrec = HierarchicalReconciliation(reconcilers=reconcilers)
    
    reconciled = hrec.reconcile(
        Y_hat_df=Y_hat_df, 
        Y_df=Y_fitted_df, 
        S_df=S, 
        tags=tags
    )

    # 4. Extract Bottom Level
    reconciled = reconciled.reset_index()
    bottom_ids = S.columns
    bottom_level = reconciled[reconciled["unique_id"].isin(bottom_ids)].copy()

    model_col_name = "Prophet/MinTrace_method_mint_shrink"
    if model_col_name not in bottom_level.columns:
        model_col_name = bottom_level.columns[-1]
        LOGGER.info(f"Using reconciled column: {model_col_name}")

    # Manual Clipping
    bottom_level["yhat"] = bottom_level[model_col_name].clip(lower=0)

    # 5. Reverse Engineering IDs
    try:
        sep = '/' if bottom_level['unique_id'].iloc[0].count('/') == len(LEVEL_COLS) - 1 else '/'
        expanded = bottom_level["unique_id"].str.split(sep, expand=True)
        expanded.columns = LEVEL_COLS

        # Reverse Prefixing
        for col in LEVEL_COLS:
            prefix = f"{col}__"
            expanded[col] = expanded[col].str.replace(prefix, "", regex=False)
            expanded[col] = expanded[col].str.replace("|", "/")

        bottom_level = pd.concat([bottom_level, expanded], axis=1)
        bottom_level["retail_week"] = pd.to_datetime(bottom_level["ds"])

        return bottom_level[LEVEL_COLS + ["retail_week", "yhat"]]
        
    except Exception as e:
        LOGGER.error(f"Error during column splitting: {e}")
        LOGGER.error(f"Sample IDs: {bottom_level['unique_id'].head().tolist()}")
        raise e


def write_forecasts(engine: Engine, sf_conf: SnowflakeConfig, forecasts: pd.DataFrame) -> None:
    """Write forecasts to Snowflake using native write_pandas."""
    
    target_table = sf_conf.target_table
    
    # Updated output columns to include new hierarchy levels
    cols_to_keep = [
        'entity_l1',  
        'prod_l1', 
        'region_l1', 
        'retail_week', 'yhat'
    ]
    out_df = forecasts[cols_to_keep].copy()
    
    out_df = out_df.rename(columns={'yhat': 'FCST'})
    out_df.columns = [c.upper() for c in out_df.columns]

    # Type Enforcement for IDs
    out_df['PROD_L1'] = pd.to_numeric(out_df['PROD_L1'], errors='coerce').fillna(0).astype(int)
    out_df['REGION_L1'] = pd.to_numeric(out_df['REGION_L1'], errors='coerce').fillna(0).astype(int)
  
    out_df["RETAIL_WEEK"] = out_df["RETAIL_WEEK"].dt.strftime('%Y-%m-%d')
    out_df['FCST'] = out_df['FCST'].astype(float)
    out_df = out_df.reset_index(drop=True)

    # Updated Schema Map
    dtype_map = {
        "ENTITY_L1": "VARCHAR",
        "PROD_L1": "INTEGER",
        "REGION_L1": "INTEGER",
        "RETAIL_WEEK": "DATE",
        "FCST": "FLOAT"
    }
    
    create_cols = ", ".join(f"{col} {dtype}" for col, dtype in dtype_map.items())
    table_name_only = target_table.split(".")[-1]

    LOGGER.info(f"Recreating table {target_table}...")
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {target_table}"))
        conn.execute(text(f"CREATE TABLE {target_table} ({create_cols})"))

    LOGGER.info(f"Writing {len(out_df)} rows to {target_table}...")
    
    raw_conn = engine.raw_connection()
    try:
        success, nchunks, nrows, _ = write_pandas(
            conn=raw_conn,
            df=out_df,
            table_name=table_name_only,
            database=sf_conf.database,
            schema=sf_conf.schema,
            quote_identifiers=False, 
            auto_create_table=False 
        )
        raw_conn.commit()
        LOGGER.info(f"Finished writing {nrows} rows.")
    except Exception as e:
        LOGGER.error(f"Failed to write pandas: {e}")
        raw_conn.rollback()
        raise e
    finally:
        raw_conn.close()


def main() -> None:
    horizon = config.FORECAST_HORIZON
    sf_conf = SnowflakeConfig.from_config()
    engine = snowflake_engine(sf_conf)

    LOGGER.info("Starting Prophet pipeline")
    history = load_history(engine, sf_conf.source_table)
    # cleaned = clean_history(history)
    forecasts = forecast(history, horizon=horizon)
    write_forecasts(engine, sf_conf, forecasts)
    LOGGER.info("Pipeline completed")


if __name__ == "__main__":
    main()

