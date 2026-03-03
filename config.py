"""
Configuration for Snowflake connection and table names.

Fill in values or load them from environment variables as preferred.
"""

import os


SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "FV80208-JN63276")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "MBOTHA@MRP.COM")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "DATA_SCIENCE_R")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "DS_SMALL_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "DATA_VAULT_DEV")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "DBT_MBOTHA")
# Optional auth modes:
# 1) Key-pair auth (recommended): provide private key path/passphrase.
SNOWFLAKE_PRIVATE_KEY_PATH = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", r"C:\Users\pdhbo\OneDrive - Mr Price Group Ltd\Documents\RLCFCST\snowflake_key.p8")
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "")
# 2) Password (fallback): SNOWFLAKE_PASSWORD
# 3) PAT/OAuth (fallback if key not provided): set SNOWFLAKE_TOKEN and SNOWFLAKE_AUTHENTICATOR=oauth
SNOWFLAKE_TOKEN = os.getenv("SNOWFLAKE_TOKEN", "")
SNOWFLAKE_AUTHENTICATOR = os.getenv("SNOWFLAKE_AUTHENTICATOR", "snowflake")
# Optional: bypass cert validation (only use if corporate proxy intercepts TLS)
SNOWFLAKE_INSECURE_MODE = os.getenv("SNOWFLAKE_INSECURE_MODE", "false").lower() in {"1", "true", "yes"}
# Optional: point Snowflake/requests at a corporate CA bundle (PEM). Defaults to local Netskope chain.
SNOWFLAKE_CA_BUNDLE = os.getenv(
    "SNOWFLAKE_CA_BUNDLE",
    os.path.join(
        os.path.dirname(__file__),
        "netskope_chain.pem",
    ),
)

# Fully qualified names are accepted, e.g. DATABASE.SCHEMA.TABLE
SOURCE_TABLE = os.getenv("SOURCE_TABLE", "RLC_GTS_HIST_WEEK")
TARGET_TABLE = os.getenv("TARGET_TABLE", "DATA_VAULT_DEV.DBT_MBOTHA.RLC_GTS_FORECAST")

# Forecast horizon in retail weeks
FORECAST_HORIZON = int(os.getenv("FORECAST_HORIZON", "52"))
