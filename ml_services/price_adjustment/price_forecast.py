from __future__ import annotations
import os
from dataclasses import dataclass
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
from sklearn.metrics import mean_absolute_error, root_mean_squared_error, r2_score

@dataclass
class Config:
    # thresholds for reliability
    mae_threshold: float = float(os.getenv("MAE_THRESHOLD", 0.5)) # maximum acceptable average error
    confidence_threshold: float = float(os.getenv("CONFIDENCE_THRESHOLD", 0.2)) # minimum confidence required

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
    """Forecast a single competitor's average price for a SKU using history windows."""
    history = load_competitor_history_windows(days=days, window_minutes=window_minutes)
    series = build_per_competitor_series(history, sku, competitor_id)
    if series.empty:
        return None

    # Create lag features
    max_lag = 1
    lag_cols = [f"lag_{i}" for i in range(1, max_lag + 1)]
    for lag in range(1, max_lag + 1):
        series[f"lag_{lag}"] = series["avg_price"].shift(lag)
    series["target"] = series["avg_price"].shift(-horizon)

    # Ensure volatility is present for all rows
    series["price_volatility"] = series["price_volatility"].fillna(0.0)
    # Drop rows with missing values
    model_df = series.dropna(subset=lag_cols + ["target"]).copy()
    if model_df.empty:
        last_val = float(series["avg_price"].iloc[-1])
        return {
            "sku": sku,
            "competitor_id": int(competitor_id),
            "horizon": horizon,
            "prediction": round(last_val, 2),
            "last_price": round(last_val, 2),
            "n_samples": 0,
            "alpha": None,
            "cv_mae": None,
            "cv_rmse": None,
            "cv_r2": None,
            "baseline_mae": None,
            "confidence": None,
            "reliable": False,
            "pred_diff": None,
            "pred_diff_pct": None,
        }

    feature_cols = lag_cols + ["price_volatility"]
    X = model_df[feature_cols]
    y = model_df["target"].astype(float)
    # Determine safe number of splits based on available samples
    # Need at least n_splits + 1 samples for TimeSeriesSplit
    n_splits = max(2, min(5, len(model_df) - 1))
    if len(model_df) < 8:
        # fallback to naive
        last_val = float(series["avg_price"].iloc[-1])
        return {
            "sku": sku,
            "competitor_id": int(competitor_id),
            "horizon": horizon,
            "prediction": round(last_val, 2),
            "last_price": round(last_val, 2),
            "n_samples": int(len(model_df)),
            "alpha": None,
            "cv_mae": None,
            "cv_rmse": None,
            "cv_r2": None,
            "baseline_mae": None,
            "confidence": None,
            "reliable": False,
            "pred_diff": None,
            "pred_diff_pct": None,
        }
    tss = TimeSeriesSplit(n_splits=n_splits)

    model = LassoCV(
        alphas=np.logspace(-3, 1, 30),
        cv=tss,
        max_iter=200000,
        tol=1e-3,
        n_jobs=-1
    )
    pipe = Pipeline([
        ("prep", ColumnTransformer([("num", StandardScaler(), feature_cols)])),
        ("model", model),
    ])

    # Cross-validation metrics
    mae_scores, rmse_scores, r2_scores = [], [], []
    baseline_mae_scores = []

    for train_idx, test_idx in tss.split(X):
        try:
            pipe.fit(X.iloc[train_idx], y.iloc[train_idx])
            pred = pipe.predict(X.iloc[test_idx])

            # Baseline: last observed value from training
            pred_base = np.repeat(y.iloc[train_idx].iloc[-1], len(test_idx))

            # Metrics for ML model
            mae_scores.append(mean_absolute_error(y.iloc[test_idx], pred))
            rmse_scores.append(root_mean_squared_error(y.iloc[test_idx], pred))
            r2_scores.append(r2_score(y.iloc[test_idx], pred))

            # Metrics for baseline
            baseline_mae_scores.append(mean_absolute_error(y.iloc[test_idx], pred_base))
        except Exception:
            continue
    # Fit final model on full dataset
    pipe.fit(X, y)

    # Predict next horizon
    last_row = series.iloc[[-1]].copy()
    last_val = float(series["avg_price"].iloc[-1])
    for lag in range(1, max_lag + 1):
        last_row[f"lag_{lag}"] = series["avg_price"].iloc[-lag]
    last_row["price_volatility"] = series["price_volatility"].iloc[-1]

    # Guard against NaN using train means so the model doesn't fail
    last_row[feature_cols] = last_row[feature_cols].fillna(X.mean(numeric_only=True))
    pred = float(pipe.predict(last_row[feature_cols])[0])

    pred_diff = pred - last_val
    pred_diff_pct = pred_diff / last_val if last_val > 0 else 0.0
    # Aggregate CV metrics
    cv_mae = np.mean(mae_scores) if mae_scores else None
    cv_rmse = np.mean(rmse_scores) if rmse_scores else None
    cv_r2 = np.mean(r2_scores) if r2_scores else None
    baseline_mae = np.mean(baseline_mae_scores) if baseline_mae_scores else None

    # Confidence: relative improvement over baseline MAE (0–1)
    confidence = None
    if baseline_mae and cv_mae:
        confidence = max(0.0, min(1.0, 1 - cv_mae / baseline_mae))

    cfg = Config()
    # Decide if prediction is reliable
    if cv_mae is not None and confidence is not None:
        reliable = cv_mae <= cfg.mae_threshold and confidence >= cfg.confidence_threshold
    else:
        reliable = False

    return {
        "sku": sku,
        "competitor_id": int(competitor_id),
        "horizon": horizon,
        "prediction": round(pred, 2),
        "confidence": round(float(confidence), 4) if confidence is not None else None,
        "last_price": round(last_val, 2),
        "n_samples": int(len(model_df)),
        "alpha": round(float(pipe.named_steps["model"].alpha_), 4),
        "cv_mae": round(float(cv_mae), 4) if cv_mae is not None else None,
        "cv_rmse": round(float(cv_rmse), 4) if cv_rmse is not None else None,
        "cv_r2": round(float(cv_r2), 3) if cv_r2 is not None else None,
        "baseline_mae": round(float(baseline_mae), 4) if baseline_mae is not None else None,
        "reliable": reliable,
        "pred_diff": round(float(pred_diff), 4) if pred_diff is not None else None,
        "pred_diff_pct": round(float(pred_diff_pct), 4) if pred_diff_pct is not None else None,
    }




