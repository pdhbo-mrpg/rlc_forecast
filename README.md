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
