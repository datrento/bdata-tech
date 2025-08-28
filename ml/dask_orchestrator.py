from __future__ import annotations

import argparse
import json
from dask import delayed, compute
from dask.distributed import Client, LocalCluster

from linear_regression import load_price_trends, predict_avg_price_for_product


def main():
    parser = argparse.ArgumentParser(description="Parallel per-SKU training orchestrated by Dask")
    parser.add_argument("--days", type=int, default=90, help="Lookback window in days")
    parser.add_argument("--horizon", type=int, default=1, help="Forecast horizon (windows ahead)")
    parser.add_argument("--competitor", type=str, default=None, help="Optional competitor name filter")
    parser.add_argument("--workers", type=int, default=None, help="Number of Dask workers (default: CPU cores)")
    parser.add_argument("--threads", type=int, default=1, help="Threads per worker")
    args = parser.parse_args()

    # Normalize competitor CLI value like "None"/"null" to actual None
    competitor = args.competitor
    if competitor is not None and str(competitor).strip().lower() in {"", "none", "null"}:
        competitor = None

    # Start a local Dask cluster (threads avoid pickling/import issues for tiny, didactic runs)
    cluster = LocalCluster(
        n_workers=(args.workers or 1),
        threads_per_worker=args.threads,
        processes=False,
        dashboard_address="127.0.0.1:8790",
    )
    client = Client(cluster)
    # dashboard not working for some reason
    print("Dask dashboard: http://127.0.0.1:8790/status")

    # Load training data once
    trends = load_price_trends(days=args.days)
    if trends.empty:
        print(json.dumps({"results": [], "note": "No data found."}))
        return

    # Get unique SKUs
    skus = (
        trends["product_sku"].dropna().astype(str).str.strip().drop_duplicates().tolist()
    )

    tasks = []
    for sku in skus:
        # Pre-slice to keep each task payload small
        df_sku = trends[trends["product_sku"].astype(str).str.strip() == sku].copy()

        @delayed
        def _run_one(sku=sku, df=df_sku):
            try:
                return predict_avg_price_for_product(
                    product_sku=sku,
                    competitor_name=competitor,
                    days=args.days,
                    horizon=args.horizon,
                    trends_df=df,
                )
            except Exception as e:
                return {
                    "product_sku": sku,
                    "competitor_name": competitor,
                    "horizon": args.horizon,
                    "prediction": None,
                    "last_observed_avg_price": None,
                    "n_samples": 0,
                    "features_used": [],
                    "alpha": None,
                    "cv_mae": None,
                    "cv_rmse": None,
                    "note": f"error: {type(e).__name__}: {e}",
                }

        tasks.append(_run_one())

    results = list(compute(*tasks))
    # Print JSON to stdout
    print(json.dumps({"results": results}, ensure_ascii=False))


if __name__ == "__main__":
    main()
