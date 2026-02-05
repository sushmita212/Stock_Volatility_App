# Stock_Volatility_App
API+Dashboard for stock volatility forecasting

This repo contains:
- `backend/`: FastAPI service that fetches OHLCV via `yfinance` and produces volatility forecasts.
- `dashboard/`: Streamlit UI that calls the FastAPI service and plots results.

## Backend (FastAPI)
Create a virtualenv, install deps, and run:

- Install:
  - `pip install -r backend/requirements.txt`
- Run:
  - `uvicorn backend.app.main:app --reload`

API endpoints:
- `GET /health`
- `POST /forecast`

Example request body:
- `{"symbol":"SPY","model":"garch11","period":"5y","interval":"1d","horizon_days":1,"realized_window":10}`

## Dashboard (Streamlit)
In another terminal:
- Install:
  - `pip install -r dashboard/requirements.txt`
- Run:
  - `streamlit run dashboard/app.py`

### Configuration
- Backend supports `.env` (see `backend/.env.example`).
- Dashboard can point to the API with `VOL_API_URL` (default `http://127.0.0.1:8000`).
