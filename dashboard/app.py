from __future__ import annotations

import os

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st


API_URL = os.getenv("VOL_API_URL", "http://127.0.0.1:8000")

st.set_page_config(page_title="Volatility Forecast", layout="wide")

st.title("Stock Volatility Forecasting")

with st.sidebar:
    st.header("Inputs")
    symbol = st.text_input("Symbol", value="SPY")
    model = st.selectbox("Model", options=["garch11", "histvol"], index=0)
    period = st.selectbox("History period", options=["1y", "2y", "5y", "10y", "max"], index=2)
    interval = st.selectbox("Interval", options=["1d", "1wk"], index=0)
    horizon_days = st.slider("Forecast horizon (days)", min_value=1, max_value=30, value=1)
    realized_window = st.slider("Realized vol window (days)", min_value=5, max_value=60, value=10)
    run = st.button("Run forecast")


def call_forecast() -> dict:
    payload = {
        "symbol": symbol,
        "model": model,
        "period": period,
        "interval": interval,
        "horizon_days": horizon_days,
        "realized_window": realized_window,
        "auto_adjust": True,
    }
    r = requests.post(f"{API_URL}/forecast", json=payload, timeout=120)
    if r.status_code != 200:
        raise RuntimeError(r.text)
    return r.json()


if run:
    with st.spinner("Forecasting..."):
        out = call_forecast()

    c1, c2, c3 = st.columns(3)
    c1.metric("Symbol", out["symbol"])
    c2.metric("Forecast vol (annualized)", f"{out['forecast_vol']:.3f}")
    if out.get("last_realized_vol") is not None:
        c3.metric("Last realized vol (annualized)", f"{out['last_realized_vol']:.3f}")
    else:
        c3.metric("Last realized vol", "n/a")

    close = pd.DataFrame(out["close_tail"]).rename(columns={"date": "Date", "close": "Close"})
    rv = pd.DataFrame(out["realized_vol_tail"]).rename(columns={"date": "Date", "realized_vol": "RealizedVol"})

    if not close.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=pd.to_datetime(close["Date"]), y=close["Close"], name="Close"))
        fig.update_layout(height=360, margin=dict(l=20, r=20, t=30, b=20), title="Close (tail)")
        st.plotly_chart(fig, use_container_width=True)

    if not rv.empty:
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=pd.to_datetime(rv["Date"]), y=rv["RealizedVol"], name="Realized vol"))
        fig2.add_hline(y=out["forecast_vol"], line_dash="dash", annotation_text="Forecast", opacity=0.6)
        fig2.update_layout(height=360, margin=dict(l=20, r=20, t=30, b=20), title="Realized vol vs forecast")
        st.plotly_chart(fig2, use_container_width=True)

    with st.expander("Raw response"):
        st.json(out)
else:
    st.info("Set inputs in the sidebar and click **Run forecast**.")
