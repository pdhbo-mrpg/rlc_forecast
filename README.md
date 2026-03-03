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
