Snowflake Streamlit app to perform prophet forecast with hierarchical reconciliation.

## Fix for `ModuleNotFoundError: forecast_pipeline_prophet`

`streamlit_app.py` now handles Snowflake environments with constrained import paths:

1. Adds the app directory (and its parent) to `sys.path`.
2. Falls back to direct file loading from `forecast_pipeline_prophet.py`.
3. Falls back **only** when `forecast_pipeline_prophet` itself is missing, so unrelated missing dependencies still fail clearly.

If your Snowflake Streamlit app fails with:

```text
ModuleNotFoundError: No module named 'forecast_pipeline_prophet'
```

set `streamlit_app.py` as your app entrypoint and stage `forecast_pipeline_prophet.py`
in the same app bundle.
Snowflake Streamlit app to perform Prophet forecasting with hierarchical reconciliation.

## Files
- `streamlit_app.py`: Native Snowflake Streamlit app that orchestrates the full workflow.
- `Reformat_history.sql`: Builds/refreshes weekly history input table.
- `forecast_pipeline_prophet.py`: Prophet + MinTrace hierarchical reconciliation logic.
- `Agg_forecast.sql`: Aggregates weekly forecasts to monthly market, ROM, and divisional outputs.
- `archive.sql`: Archives existing monthly forecast snapshots.

## Streamlit workflow
The Streamlit app executes this sequence:
1. Conditionally creates archive table (if missing) and executes archive logic.
2. Runs `Reformat_history.sql`.
3. Loads history and executes Prophet + hierarchical reconciliation.
4. Runs `Agg_forecast.sql`.
