from __future__ import annotations

import os
import json
from dataclasses import dataclass
from typing import Optional, Dict, Any, List

import numpy as np
import pandas as pd
from sqlalchemy import text
import datetime

from dask import delayed, compute
from dask.distributed import Client, LocalCluster
from config import get_engine
from price_forecast import forecast_competitor_price

# -------------------------
# Config
# -------------------------
@dataclass
class Config:
    db_url: str = os.getenv(
        "POSTGRES_URL",
        "postgresql+psycopg2://postgres:postgres@postgres:5432/price_intelligence",
    )
    price_grid_pct: float = float(os.getenv("PRICE_GRID_PCT", "0.05")) # 5% grid around the current price
    min_margin_pct: float = float(os.getenv("MIN_MARGIN_PCT", "0.05")) # 5% margin 
    dask_workers: int = int(os.getenv("DASK_WORKERS", "2"))
    run_created_by: str = "price-proposer"
    dask_dashboard_address: str = os.getenv("DASK_DASHBOARD", "127.0.0.1:8791")
    # Forecast-guided anchoring (simple, minimal knobs)
    target_undercut_pct: float = float(os.getenv("TARGET_UNDERCUT_PCT", "0.01")) # 1% undercut of the forecasted cheapest price
    gap_tolerance_pct: float = float(os.getenv("GAP_TOLERANCE_PCT", "0.003")) # 0.3% gap tolerance for the forecasted cheapest price
    forecast_lookback_days: int = int(os.getenv("FORECAST_LOOKBACK_DAYS", "14")) # 14 days
    demand_lookback_minutes: int = int(os.getenv("DEMAND_LOOKBACK_MINUTES", "60")) # 60 minutes

def fetch_active_skus(engine) -> List[str]:
    df = pd.read_sql(
        text(
            """
            SELECT sku FROM platform_products
            WHERE is_active = TRUE AND in_stock = TRUE
            """
        ),
        engine,
    )
    return df["sku"].astype(str).tolist()


def load_features(engine, sku: str) -> Optional[Dict[str, Any]]:
    q = text(
        """
        SELECT sku, current_price, cost, min_viable_price,
               COALESCE(price_elasticity, -1.0) AS elasticity
        FROM platform_products
        WHERE sku = :sku
        """
    )
    df = pd.read_sql(q, engine, params={"sku": sku})
    if df.empty:
        return None
    r = df.iloc[0]
    return {
        "sku": r["sku"],
        "price": float(r["current_price"]),
        "cost": float(r["cost"]),
        "min_viable_price": (float(r["min_viable_price"]) if not pd.isna(r["min_viable_price"]) else None),
        "elasticity": (float(r["elasticity"]) if not pd.isna(r["elasticity"]) else -1.0),
    }


def fetch_competitor_ids(engine, sku: str) -> List[int]:
    df = pd.read_sql(
        text(
            """
            SELECT DISTINCT competitor_id
            FROM competitor_price_history
            WHERE product_sku = :sku AND collection_timestamp > NOW() - INTERVAL '60 days'
            """
        ),
        engine,
        params={"sku": sku},
    )
    return (
        df["competitor_id"].dropna().astype(int).drop_duplicates().tolist()
        if not df.empty
        else []
    )


def get_forecasted_average(engine, sku: str, lookback_days: int) -> tuple[dict | None, list[dict]]:
    competitor_ids = fetch_competitor_ids(engine, sku)
    if not competitor_ids:
        return None, []
    # Parallelize forecasts across competitors via Dask
    tasks = [delayed(forecast_competitor_price)(sku, int(cid), days=lookback_days, horizon=1, window_minutes=5) for cid in competitor_ids]
    results = list(compute(*tasks)) if tasks else []
    predictions = [
        r for r in results
        if r and r.get("prediction") is not None and r.get("reliable", False)
    ]

    if not predictions:
        return None, []
    return min(predictions, key=lambda r: float(r["prediction"])), results


def load_demand_baseline(engine, sku: str, demand_lookback_minutes: int) -> float:
    """Compute a smoothed demand baseline using EMA over recent purchase rates (last demand_lookback_minutes minutes)."""
    q = text(
        f"""
        SELECT window_start, window_end, purchases
        FROM user_behavior_summary
        WHERE product_sku = :sku AND window_end > NOW() - INTERVAL '{demand_lookback_minutes} minutes'
        ORDER BY window_end
        """
    )
    df = pd.read_sql(q, engine, params={"sku": sku})
    if df.empty:
        return 1.0
    df["window_start"] = pd.to_datetime(df["window_start"], errors="coerce")
    df["window_end"] = pd.to_datetime(df["window_end"], errors="coerce")
    df = df.dropna(subset=["window_start", "window_end"]).copy()
    if df.empty:
        return 1.0
    window_minutes = (df["window_end"] - df["window_start"]).dt.total_seconds() / 60.0
    window_minutes = window_minutes.replace(0, 5.0).fillna(5.0)
    df["rate_per_hour"] = df["purchases"].astype(float) * (60.0 / window_minutes)
    d0 = df["rate_per_hour"].ewm(halflife=60, adjust=False).mean().iloc[-1]
    return float(d0) if np.isfinite(d0) and d0 > 0 else 1.0


# -------------------------
# Pricing logic not making the candidate price below the min viable price
# -------------------------
def clamp_price(candidate_price: float, cost: float, min_viable: Optional[float], min_margin_pct: float) -> float:
    margin_floor = cost * (1.0 + min_margin_pct)
    lower_bound = max(margin_floor, min_viable or margin_floor)
    return max(candidate_price, lower_bound)


def propose_for_sku(cfg: Config, feat: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    initial_price = feat["price"]
    cost = feat["cost"]
    elasticity = feat.get("elasticity", -1.0) or -1.0
    min_viable_price = feat.get("min_viable_price", None)

    initial_demand = float(feat.get("demand_baseline", 1.0))

    # If a confident forecasted cheapest is available, directly target band around it
    forecast_average_price = feat.get("forecast_average_price", None)
    forecast_reliability = bool(feat.get("forecast_reliability", False))
    if forecast_average_price is not None and forecast_reliability:
        predicted_average_price = float(forecast_average_price)
        target_price = predicted_average_price * (1.0 - cfg.target_undercut_pct)
        band_low = target_price * (1.0 - cfg.gap_tolerance_pct)
        band_high = target_price * (1.0 + cfg.gap_tolerance_pct)
        candidate_price = min(max(target_price, band_low), band_high)
        candidate_price = clamp_price(candidate_price, cost, min_viable_price, cfg.min_margin_pct)

        demand = initial_demand * (candidate_price / initial_price) ** elasticity
        profit = max(0.0, candidate_price - cost) * demand
        gap_after = (
            (candidate_price - predicted_average_price) / predicted_average_price
            if float(predicted_average_price) != 0.0 else None
        )

        return {
            "sku": feat["sku"],
            "old_price": round(float(initial_price), 2),
            "proposed_price": round(float(candidate_price), 2),
            "elasticity_used": elasticity,
            "expected_demand_delta": float(demand - initial_demand),
            "expected_profit_delta": float(profit - (max(0.0, initial_price - cost) * initial_demand)),
            "competitor_gap_after": float(gap_after),
            "score": float(profit),
            "reason_code": "elasticity+forecast",
            "constraints": {
                "min_margin_pct": cfg.min_margin_pct,
                "target_undercut_pct": cfg.target_undercut_pct,
                "gap_tolerance_pct": cfg.gap_tolerance_pct,
                "forecast_reliability": forecast_reliability,
                "demand_baseline_minutes": cfg.demand_lookback_minutes,
                "demand_ema_halflife_minutes": 60,
            },
        }

    # Fallback: explore grid around current price using elasticity-only profit
    best = None
    possible_deltas = np.linspace(-cfg.price_grid_pct, cfg.price_grid_pct, 11)
    for delta in possible_deltas:
        candidate_price = initial_price * (1.0 + float(delta))
        candidate_price = clamp_price(candidate_price, cost, min_viable_price, cfg.min_margin_pct)

        # calculate the demand and profit for the candidate price
        # the demand is calculated using the elasticity of demand
        # the profit is calculated as the difference between the candidate price and the cost, multiplied by the demand
        demand = initial_demand * (candidate_price / initial_price) ** elasticity
        profit = max(0.0, candidate_price - cost) * demand
        cand = {
            "proposed_price": round(float(candidate_price), 2),
            "score": float(profit),
            "expected_profit_delta": float(profit - (max(0.0, initial_price - cost) * initial_demand)),
            "expected_demand_delta": float(demand - initial_demand),
        }
        if best is None or cand["score"] > best["score"]:
            best = cand

    if not best:
        return None

    return {
        "sku": feat["sku"],
        "old_price": round(float(initial_price), 2),
        "proposed_price": best["proposed_price"],
        "elasticity_used": elasticity,
        "expected_demand_delta": best["expected_demand_delta"],
        "expected_profit_delta": best["expected_profit_delta"],
        "competitor_gap_after": None,
        "score": best["score"], # it's the profit
        "reason_code": "elasticity-only",
        "constraints": {
            "min_margin_pct": cfg.min_margin_pct,
            "price_grid_pct": cfg.price_grid_pct,
            "demand_baseline_minutes": 60,
            "demand_ema_halflife_minutes": 60,
        },
    }


# -------------------------
# Persistence
# -------------------------
def create_run(engine, cfg: Config, candidates_count: int) -> int:
    with engine.begin() as conn:
        res = conn.execute(
            text(
                """
                INSERT INTO price_adjustment_runs (parameters, candidates_count, proposed_count, created_by)
                VALUES (CAST(:params AS JSONB), :candidates, 0, :by)
                RETURNING id
                """
            ),
            {
                "params": json.dumps({
                    "price_grid_pct": cfg.price_grid_pct,
                }),
                "candidates": candidates_count,
                "by": cfg.run_created_by,
            },
        )
        return int(res.scalar_one())


def finalize_run(engine, run_id: int, proposed_count: int):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE price_adjustment_runs
                SET proposed_count = :n, finished_at = NOW()
                WHERE id = :run_id
                """
            ),
            {"run_id": run_id, "n": proposed_count},
        )


def insert_proposals(engine, run_id: int, proposals: List[Dict[str, Any]], created_by: str):
    if not proposals:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO price_adjustments (
                    sku, old_price, proposed_price, elasticity_used,
                    expected_demand_delta, expected_profit_delta,
                    competitor_gap_after, score, reason_code, constraints,
                    status, run_id, created_by
                ) VALUES (
                    :sku, :old_price, :proposed_price, :elasticity_used,
                    :expected_demand_delta, :expected_profit_delta,
                    :competitor_gap_after, :score, :reason_code, CAST(:constraints AS JSONB),
                    'pending', :run_id, :created_by
                )
                """
            ),
            [
                {
                    **p,
                    "constraints": json.dumps(p.get("constraints", {})),
                    "run_id": run_id,
                    "created_by": created_by,
                }
                for p in proposals
            ],
        )

def insert_forecast_metrics(engine, run_id: int, forecasts: List[Dict[str, Any]]):
    if not forecasts:
        return
    import numpy as np

    def to_number(value: Any) -> float | None:
        if value is None:
            return None
        try:
            v = float(value)
            if not np.isfinite(v):
                return None
            return v
        except Exception:
            return None

    params: List[Dict[str, Any]] = []
    for f in forecasts:
        ns_raw = f.get("n_samples", f.get("num_samples", 0))
        try:
            n_samples = int(ns_raw or 0)
        except Exception:
            n_samples = 0
        if n_samples <= 0:
            continue

        sku = f.get("sku")
        comp = f.get("competitor_id")
        hor = f.get("horizon", 1)
        if sku is None or comp is None:
            continue
        try:
            competitor_id = int(comp)
            horizon = int(hor)
        except Exception:
            continue

        row = {
            "run_id": int(run_id),
            "sku": str(sku),
            "competitor_id": competitor_id,
            "horizon": horizon,
            "prediction": to_number(f.get("prediction")),
            "last_price": to_number(f.get("last_price")),
            "n_samples": n_samples,
            "alpha": to_number(f.get("alpha")),
            "cv_mae": to_number(f.get("cv_mae")),
            "cv_rmse": to_number(f.get("cv_rmse")),
            "cv_r2": to_number(f.get("cv_r2")),
            "baseline_mae": to_number(f.get("baseline_mae")),
            "confidence": to_number(f.get("confidence")),
            "reliable": bool(f.get("reliable", False)),
            "pred_diff": to_number(f.get("pred_diff")),
            "pred_diff_pct": to_number(f.get("pred_diff_pct")),
        }
        params.append(row)

    if not params:
        return

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO forecast_metrics (
                    run_id, sku, competitor_id, horizon, prediction, last_price,
                    n_samples, alpha, cv_mae, cv_rmse, cv_r2, baseline_mae,
                    confidence, reliable, pred_diff, pred_diff_pct
                ) VALUES (
                    :run_id, :sku, :competitor_id, :horizon, :prediction, :last_price,
                    :n_samples, :alpha, :cv_mae, :cv_rmse, :cv_r2, :baseline_mae,
                    :confidence, :reliable, :pred_diff, :pred_diff_pct
                )
                """
            ),
            params,
        )


def main():
    cfg = Config()
    engine = get_engine()
    # gate the run only if there are new data
    # the run is skipped if the elapsed time is less than 12 hours and the number of new rows is less than 100
    # the guard is to prevent the run from being run too frequently
    MIN_ELAPSED_MIN = int(os.getenv("GUARD_MIN_ELAPSED_MIN", "720"))   # 12h
    MIN_NEW_ROWS = int(os.getenv("GUARD_MIN_NEW_ROWS", "100")) # 100 new rows

    with engine.connect() as conn:
        last_run_ts = conn.execute(text("SELECT MAX(finished_at) FROM price_adjustment_runs")).scalar()
        if last_run_ts is not None:
            elapsed_min = conn.execute(
                text("SELECT EXTRACT(EPOCH FROM (NOW() - MAX(finished_at))) / 60.0 FROM price_adjustment_runs")
            ).scalar() or 0.0
            new_rows = conn.execute(
                text("SELECT COUNT(*) FROM competitor_price_history WHERE collection_timestamp > (SELECT MAX(finished_at) FROM price_adjustment_runs)")
            ).scalar() or 0
            if elapsed_min < MIN_ELAPSED_MIN and new_rows < MIN_NEW_ROWS:
                print({"skip": True, "reason": "guard", "elapsed_min": elapsed_min, "new_rows": new_rows})
                return

    skus = fetch_active_skus(engine)
    run_id = create_run(engine, cfg, candidates_count=len(skus))

    cluster = LocalCluster(
        n_workers=cfg.dask_workers,
        threads_per_worker=1,
        processes=False,
        dashboard_address=cfg.dask_dashboard_address,
    )
    Client(cluster)

    forecasts = []  # collect forecasts for all SKUs

    tasks = []
    for sku in skus:
        feat = load_features(engine, sku)
        if not feat:
            continue
        # attach demand baseline from recent behavior over the last 60 minutes
        try:
            feat["demand_baseline"] = load_demand_baseline(engine, sku, cfg.demand_lookback_minutes)
        except Exception:
            feat["demand_baseline"] = 1.0

        # attach forecasted cheapest (if any, confidence-gated)
        try:
            cheap_forecasted_price, forecasts_for_sku = get_forecasted_average(engine, sku, cfg.forecast_lookback_days)
            if cheap_forecasted_price:
                feat["forecast_average_price"] = cheap_forecasted_price["prediction"]
                feat["forecast_reliability"] = cheap_forecasted_price.get("reliable", False)
            forecasts.extend(forecasts_for_sku)
        except Exception:
            pass

        @delayed
        def _work(f=feat):
            return propose_for_sku(cfg, f)

        tasks.append(_work())

    results = list(compute(*tasks)) if tasks else []
    proposals = [r for r in results if r]

    # Store forecast metrics (all forecasts, including unreliable ones) but only the ones with by ml model
    insert_forecast_metrics(engine, run_id, forecasts)

    # store the proposals and finalize the run
    insert_proposals(engine, run_id, proposals, created_by=cfg.run_created_by)
    finalize_run(engine, run_id, proposed_count=len(proposals))

    print(json.dumps({"run_id": run_id, "candidates": len(skus), "proposals": len(proposals), "forecasts": len(forecasts)}))


if __name__ == "__main__":
    main()


