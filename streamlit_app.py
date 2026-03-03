"""Snowflake Streamlit entrypoint for the forecast pipeline.

This app resolves `forecast_pipeline_prophet` from staged files even when
Snowflake's default module path does not include the app root.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import streamlit as st


def _load_forecast():
    """Load and return `forecast` from `forecast_pipeline_prophet`."""
    app_root = Path(__file__).resolve().parent
    for candidate in (app_root, app_root.parent):
        candidate_str = str(candidate)
        if candidate_str not in sys.path:
            sys.path.insert(0, candidate_str)

    try:
        from forecast_pipeline_prophet import forecast as loaded_forecast
        return loaded_forecast
    except ModuleNotFoundError as exc:
        # Only fall back when the target module itself is not found.
        if exc.name != "forecast_pipeline_prophet":
            raise

        module_path = app_root / "forecast_pipeline_prophet.py"
        if not module_path.is_file():
            raise ModuleNotFoundError(
                "No module named 'forecast_pipeline_prophet'. "
                "Ensure forecast_pipeline_prophet.py is staged with streamlit_app.py."
            ) from exc

        spec = importlib.util.spec_from_file_location("forecast_pipeline_prophet", module_path)
        if spec is None or spec.loader is None:
            raise ModuleNotFoundError("Unable to load forecast_pipeline_prophet module") from exc

        module = importlib.util.module_from_spec(spec)
        sys.modules["forecast_pipeline_prophet"] = module
        spec.loader.exec_module(module)
        return module.forecast


forecast = _load_forecast()

st.title("Retail Forecast App")
