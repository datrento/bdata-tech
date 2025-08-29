from __future__ import annotations

import os
import json
from dataclasses import dataclass
from typing import Optional, Dict, Any, List

import numpy as np
import pandas as pd
from sqlalchemy import text

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
    forecast_min_conf: float = float(os.getenv("FORECAST_MIN_CONF", "0.6")) # 60% confidence

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


def get_forecasted_cheapest(engine, sku: str, min_conf: float) -> Optional[Dict[str, Any]]:
    competitor_ids = fetch_competitor_ids(engine, sku)
    predictions: List[Dict[str, Any]] = []
    for cid in competitor_ids:
        result = forecast_competitor_price(sku, cid, days=60, horizon=1, window_minutes=5)
        if result and result.get("prediction") is not None and float(result.get("confidence", 0.0)) >= float(min_conf):
            predictions.append(result)
    if not predictions:
        return None
    return min(predictions, key=lambda r: float(r["prediction"]))


def load_demand_baseline(engine, sku: str) -> float:
    """Compute a smoothed demand baseline using EMA over recent purchase rates (last 60m)."""
    q = text(
        """
        SELECT window_start, window_end, purchases
        FROM user_behavior_summary
        WHERE product_sku = :sku AND window_end > NOW() - INTERVAL '60 minutes'
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
# Pricing core (minimal)
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
    forecast_cheapest = feat.get("forecast_cheapest", None)
    forecast_conf = float(feat.get("forecast_conf", 0.0))
    if forecast_cheapest is not None and forecast_conf >= cfg.forecast_min_conf:
        cheapest = float(forecast_cheapest)
        target_price = cheapest * (1.0 - cfg.target_undercut_pct)
        band_low = target_price * (1.0 - cfg.gap_tolerance_pct)
        band_high = target_price * (1.0 + cfg.gap_tolerance_pct)
        candidate_price = min(max(target_price, band_low), band_high)
        candidate_price = clamp_price(candidate_price, cost, min_viable_price, cfg.min_margin_pct)

        demand = initial_demand * (candidate_price / initial_price) ** elasticity
        profit = max(0.0, candidate_price - cost) * demand
        gap_after = (candidate_price - cheapest) / cheapest

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
                "forecast_confidence": forecast_conf,
                "demand_baseline_minutes": 60,
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


# -------------------------
# Orchestration (Dask)
# -------------------------
def main():
    cfg = Config()
    engine = get_engine()
    skus = fetch_active_skus(engine)
    run_id = create_run(engine, cfg, candidates_count=len(skus))

    cluster = LocalCluster(
        n_workers=cfg.dask_workers,
        threads_per_worker=1,
        processes=False,
        dashboard_address=cfg.dask_dashboard_address,
    )
    Client(cluster)

    tasks = []
    for sku in skus:
        feat = load_features(engine, sku)
        if not feat:
            continue
        # attach demand baseline from recent behavior over the last 60 minutes
        try:
            feat["demand_baseline"] = load_demand_baseline(engine, sku)
        except Exception:
            feat["demand_baseline"] = 1.0

        # attach forecasted cheapest (if any, confidence-gated)
        try:
            fc = get_forecasted_cheapest(engine, sku, cfg.forecast_min_conf)
            if fc:
                feat["forecast_cheapest"] = fc["prediction"]
                feat["forecast_conf"] = fc.get("confidence", 0.0)
        except Exception:
            pass

        @delayed
        def _work(f=feat):
            return propose_for_sku(cfg, f)

        tasks.append(_work())

    results = list(compute(*tasks)) if tasks else []
    proposals = [r for r in results if r]

    insert_proposals(engine, run_id, proposals, created_by=cfg.run_created_by)
    finalize_run(engine, run_id, proposed_count=len(proposals))

    print(json.dumps({"run_id": run_id, "candidates": len(skus), "proposals": len(proposals)}))


if __name__ == "__main__":
    main()


