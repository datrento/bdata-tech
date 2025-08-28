import pandas as pd
from sqlalchemy import text
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dashboard.db import get_db_connection  # Reuse existing connector
import numpy as np
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LassoCV
from sklearn.metrics import mean_absolute_error, root_mean_squared_error


def load_price_trends(days: int = 30) -> pd.DataFrame:
    engine = get_db_connection()
    q = text(f"""
        SELECT 
            product_sku,
            competitor_name,
            window_start,
            window_end,
            avg_price,
            price_volatility,
            trend_direction
        FROM price_trends
        WHERE window_end > NOW() - INTERVAL '{days} days'
        ORDER BY window_start
    """)
    return pd.read_sql(q, engine)


def predict_avg_price_for_product(
    product_sku: str,
    *,
    competitor_name: str | None = None,
    days: int = 90,
    horizon: int = 1,
    alphas: np.ndarray | None = None,
    trends_df: pd.DataFrame | None = None,
) -> dict:
    """
    Predict the next-window average price for a single product using a simple
    Lasso regression (with CV for alpha and built-in feature selection).

    Parameters:
    - product_sku: SKU to predict for.
    - competitor_name: Optional competitor filter. If None, train across all
      competitors for this SKU and include competitor_name as a categorical feature.
    - days: Lookback window for training data from price_trends.
    - horizon: Forecast horizon in number of windows ahead (default 1).
    - alphas: Optional array of alpha values for LassoCV. If None, uses logspace.

    Returns dict with keys: product_sku, competitor_name, horizon, prediction,
    last_observed_avg_price, n_samples, features_used, alpha, cv_mae, cv_rmse.
    """
    if alphas is None:
        alphas = np.logspace(-3, 1, 30)

    # Allow passing a preloaded DataFrame to avoid re-reading from DB in orchestrated runs
    trends = trends_df if trends_df is not None else load_price_trends(days=days)
    if trends.empty:
        raise ValueError("No trend data available for training.")

    # Filter by product (normalize to avoid subtle mismatches)
    df = trends[trends["product_sku"].astype(str).str.strip() == str(product_sku).strip()].copy()
    if df.empty:
        raise ValueError(f"No trend data found for product_sku={product_sku}.")

    # Optional competitor filter
    if competitor_name is not None:
        df = df[df["competitor_name"].astype(str).str.lower().str.strip() == str(competitor_name).lower().strip()]
        if df.empty:
            raise ValueError(
                f"No trend data for product_sku={product_sku} and competitor_name={competitor_name}."
            )

    # Sort by time and ensure datetime
    df["window_end"] = pd.to_datetime(df["window_end"], errors="coerce", utc=True)
    df = df.dropna(subset=["window_end"]).sort_values("window_end").reset_index(drop=True)

    # common way to let a linear model “see” recent history: lags of avg_price and price_volatility
    max_lag = 3
    for lag in range(1, max_lag + 1):
        df[f"avg_price_lag{lag}"] = df["avg_price"].shift(lag)
        df[f"price_volatility_lag{lag}"] = df["price_volatility"].shift(lag)

    # Encode trend_direction as categorical; competitor_name only if not filtered
    df["trend_direction"] = df["trend_direction"].astype(str)
    if competitor_name is None:
        df["competitor_name_norm"] = df["competitor_name"].astype(str).str.lower().str.strip()

    # Target is future avg price
    df["target_avg_price"] = df["avg_price"].shift(-horizon)

    # Drop rows without full feature/target data
    feature_cols_num = [
        f"avg_price_lag{lag}" for lag in range(1, max_lag + 1)
    ] + [
        f"price_volatility_lag{lag}" for lag in range(1, max_lag + 1)
    ]
    feature_cols_cat = ["trend_direction"] + (["competitor_name_norm"] if competitor_name is None else [])

    model_df = df.dropna(subset=feature_cols_num + ["target_avg_price"]).copy()
    if model_df.empty or len(model_df) < 10:
        # Fallback: naive forecast = last observed avg_price
        last_avg = float(df["avg_price"].iloc[-1])
        return {
            "product_sku": product_sku,
            "competitor_name": competitor_name,
            "horizon": horizon,
            "prediction": round(last_avg, 2),
            "last_observed_avg_price": round(last_avg, 2),
            "n_samples": int(len(model_df)),
            "features_used": feature_cols_num + feature_cols_cat,
            "alpha": None,
            "cv_mae": None,
            "cv_rmse": None,
            "note": "Insufficient data; returned naive last value.",
        }

    X = model_df[feature_cols_num + feature_cols_cat]
    y = model_df["target_avg_price"].astype(float)

    # Preprocess: scale numeric, one-hot categorical
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), feature_cols_num),
            ("cat", OneHotEncoder(handle_unknown="ignore"), feature_cols_cat),
        ]
    )

    # Use the same TimeSeriesSplit for model CV and metrics
    n_splits = min(5, max(2, len(model_df) // 5))
    tss = TimeSeriesSplit(n_splits=n_splits)

    model = LassoCV(
        alphas=alphas,
        cv=tss,
        max_iter=100000,
        tol=1e-3,
        n_jobs=-1,
    )
    pipe = Pipeline(steps=[("prep", preprocessor), ("model", model)])

    # TimeSeriesSplit CV for metric estimation
    maes, rmses = [], []
    for train_idx, test_idx in tss.split(X):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
        pipe.fit(X_train, y_train)
        y_pred = pipe.predict(X_test)
        maes.append(mean_absolute_error(y_test, y_pred))
        rmses.append(root_mean_squared_error(y_test, y_pred))

    # Fit on all data to forecast next horizon
    pipe.fit(X, y)

    # Build the last feature row to forecast next window
    last_row = df.iloc[[-1]].copy()
    for lag in range(1, max_lag + 1):
        last_row[f"avg_price_lag{lag}"] = df["avg_price"].iloc[-lag]
        last_row[f"price_volatility_lag{lag}"] = df["price_volatility"].iloc[-lag]
    last_row["trend_direction"] = last_row["trend_direction"].astype(str)
    if competitor_name is None:
        last_row["competitor_name_norm"] = last_row["competitor_name"].astype(str).str.lower().str.strip()

    X_next = last_row[feature_cols_num + feature_cols_cat]
    pred = float(pipe.predict(X_next)[0])

    return {
        "product_sku": product_sku,
        "competitor_name": competitor_name,
        "horizon": horizon,
        "prediction": round(pred, 2),
        "last_observed_avg_price": round(float(df["avg_price"].iloc[-1]), 2),
        "n_samples": int(len(model_df)),
        "features_used": feature_cols_num + feature_cols_cat,
        "alpha": round(float(pipe.named_steps["model"].alpha_), 2),
        "cv_mae": (round(float(np.mean(maes)), 2) if maes else None),
        "cv_rmse": (round(float(np.mean(rmses)), 2) if rmses else None),
    }

if __name__ == "__main__":
    # Example usage
    result = predict_avg_price_for_product("PS5-CONSOLE", competitor_name=None, days=90, horizon=1)
    print(result)