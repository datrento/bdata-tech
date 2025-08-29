from __future__ import annotations

from typing import Dict, Any

import numpy as np
import pandas as pd
from sqlalchemy import text
from config import get_engine
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LassoCV
from sklearn.metrics import mean_absolute_error, root_mean_squared_error

def load_competitor_history_windows(days: int = 30, window_minutes: int = 5) -> pd.DataFrame:
    """Aggregate competitor_price_history into fixed windows per SKU+competitor."""
    engine = get_engine()
    # Build a bucketed window_start aligned to window_minutes
    q = text(
        f"""
        WITH b AS (
          SELECT 
            product_sku,
            competitor_id,
            collection_timestamp,
            price,
            date_trunc('hour', collection_timestamp)
              + floor(date_part('minute', collection_timestamp)/{window_minutes}) * interval '{window_minutes} minute' as window_start
          FROM competitor_price_history
          WHERE collection_timestamp > NOW() - INTERVAL '{days} days'
        )
        SELECT 
          product_sku,
          competitor_id,
          window_start,
          window_start + interval '{window_minutes} minute' as window_end,
          AVG(price) as avg_price,
          STDDEV_SAMP(price) as price_volatility
        FROM b
        GROUP BY product_sku, competitor_id, window_start
        ORDER BY window_start
        """
    )
    return pd.read_sql(q, engine)

def build_per_competitor_series(history_df: pd.DataFrame, sku: str, competitor_id: int) -> pd.DataFrame:
    df = history_df[
        (history_df["product_sku"].astype(str).str.strip() == str(sku).strip())
        & (history_df["competitor_id"].astype(int) == int(competitor_id))
    ].copy()
    if df.empty:
        return df
    df["window_end"] = pd.to_datetime(df["window_end"], errors="coerce")
    df = df.dropna(subset=["window_end"]).sort_values("window_end")
    # ensure volatility present
    if "price_volatility" not in df.columns:
        df["price_volatility"] = 0.0
    return df


def forecast_competitor_price(
    sku: str,
    competitor_id: int,
    *,
    days: int = 60,
    horizon: int = 1,
    window_minutes: int = 5,
) -> Dict[str, Any]:
    """Forecast a single competitor's avg price for a SKU using history windows."""
    history = load_competitor_history_windows(days=days, window_minutes=window_minutes)
    series = build_per_competitor_series(history, sku, competitor_id)
    if series.empty:
        return None

    max_lag = 4 # shifing the price by 1, 2, 3, 4 
    for lag in range(1, max_lag + 1):
        series[f"lag_{lag}"] = series["avg_price"].shift(lag)
    series["target"] = series["avg_price"].shift(-horizon)
    model_df = series.dropna(subset=[f"lag_{i}" for i in range(1, max_lag + 1)] + ["target"]).copy()
    if model_df.empty:
        last_val = float(series["avg_price"].iloc[-1])
        return {
            "sku": sku,
            "competitor_id": int(competitor_id),
            "horizon": horizon,
            "prediction": round(last_val, 2),
            "last": round(last_val, 2),
            "n_samples": 0,
            "alpha": None,
            "cv_mae": None,
            "cv_rmse": None,
            "confidence": 0.0,
            "note": "no feature rows; naive",
        }

    feature_cols_num = [f"lag_{i}" for i in range(1, max_lag + 1)] + ["price_volatility"]
    X = model_df[feature_cols_num]
    y = model_df["target"].astype(float)
    tss = TimeSeriesSplit(n_splits=min(5, max(2, len(model_df) // 5)))
    model = LassoCV(alphas=np.logspace(-3, 1, 30), cv=tss, max_iter=200000, tol=1e-3, n_jobs=-1)
    pipe = Pipeline([
        ("prep", ColumnTransformer([
            ("num", StandardScaler(), feature_cols_num),
        ])),
        ("model", model),
    ])

    # cross-validation to get the best alpha
    maes, rmses = [], []
    for tr, te in tss.split(X):
        pipe.fit(X.iloc[tr], y.iloc[tr])
        pred = pipe.predict(X.iloc[te])
        maes.append(mean_absolute_error(y.iloc[te], pred))
        rmses.append(root_mean_squared_error(y.iloc[te], pred))

    # fit the model to the entire dataset
    pipe.fit(X, y)

    # make a prediction for the last price
    last = series.iloc[[-1]].copy()
    for lag in range(1, max_lag + 1):
        last[f"lag_{lag}"] = series["avg_price"].iloc[-lag]
    last["price_volatility"] = series["price_volatility"].iloc[-1]
    pred = float(pipe.predict(last[feature_cols_num])[0])

    cv_rmse = float(np.mean(rmses)) if rmses else None
    # Normalize error by a robust scale (median of target)
    price_scale = max(1e-6, float(np.median(model_df["target"])) )
    nrmse = (cv_rmse / price_scale) if cv_rmse is not None else 1.0
    # Clamp into [0, 1]
    confidence = max(0.0, 1.0 - min(1.0, float(nrmse)))

    return {
        "sku": sku,
        "competitor_id": int(competitor_id),
        "horizon": horizon,
        "prediction": round(pred, 2),
        "last": round(last_val, 2),
        "n_samples": int(len(model_df)),
        "alpha": round(float(pipe.named_steps["model"].alpha_), 4),
        "cv_mae": round(float(np.mean(maes)), 4) if maes else None,
        "cv_rmse": round(float(cv_rmse), 4) if cv_rmse is not None else None,
        "confidence": round(float(confidence), 3),
    }



