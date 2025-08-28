import os
import time
import logging
from typing import Dict, List, Optional, Tuple
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from datetime import datetime


def get_env(name: str, default: str = "") -> str:
    value = os.getenv(name, default)
    if value is None:
        return default
    return value


def build_pg_dsn() -> str:
    # SQLAlchemy DSN using psycopg2 driver
    user = get_env("POSTGRES_USER")
    password = get_env("POSTGRES_PASSWORD")
    host = get_env("POSTGRES_HOST")
    port = get_env("POSTGRES_PORT")
    db = get_env("POSTGRES_DB")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"

def create_db_engine() -> Engine:
    dsn = build_pg_dsn()
    # pool_pre_ping ensures dead connections are detected
    return create_engine(dsn, pool_pre_ping=True, future=True)


def ensure_tables_exist(engine: Engine):
    # Create observations table for elasticity if it doesn't exist
    create_sql = text(
        """
        CREATE TABLE IF NOT EXISTS price_demand_observations (
            id BIGSERIAL PRIMARY KEY,
            sku VARCHAR(100) NOT NULL,
            observed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            price DECIMAL(10,2) NOT NULL,
            demand INT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_pdo_sku_time ON price_demand_observations (sku, observed_at DESC);
        """
    )
    with engine.begin() as conn:
        conn.execute(create_sql)
        # Idempotency for observations per SKU+time
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_pdo_sku_obs
            ON price_demand_observations (sku, observed_at);
        """))


def get_last_observed_map(engine: Engine) -> Dict[str, datetime]:
    q = text(
        """
        SELECT sku, MAX(observed_at) AS last_obs
        FROM price_demand_observations
        GROUP BY sku
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(q).all()
    return {r[0]: r[1] for r in rows if r[1] is not None}


def collect_and_store_observations(engine: Engine, logger: logging.Logger, rollup_minutes: int = 60) -> int:
    """Aggregate purchases from user_behavior_summary over a recent window and
    write one observation per SKU using observed_at = latest window_end in the rollup.
    Returns number of rows inserted.
    """
    # Aggregate demand from summary windows in the last N minutes
    agg_sql = text(
        """
        WITH recent AS (
            SELECT product_sku, window_end, purchases
            FROM user_behavior_summary
            WHERE window_end > NOW() - ((:m || ' minutes')::interval)
        ),
        rollup AS (
            SELECT product_sku,
                   MAX(window_end) AS observed_at,
                   SUM(COALESCE(purchases, 0))::BIGINT AS demand
            FROM recent
            GROUP BY product_sku
        )
        SELECT ru.product_sku,
               ru.observed_at,
               ru.demand,
               COALESCE(h.adjusted_price, pp.current_price) AS price_at_obs
        FROM rollup ru
        JOIN platform_products pp ON pp.sku = ru.product_sku AND pp.is_active = TRUE
        LEFT JOIN LATERAL (
            SELECT adjusted_price
            FROM platform_product_price_history h
            WHERE h.sku = ru.product_sku
              AND h.change_timestamp <= ru.observed_at
            ORDER BY h.change_timestamp DESC
            LIMIT 1
        ) h ON TRUE
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(agg_sql, {"m": str(rollup_minutes)}).all()

    if not rows:
        return 0

    last_obs = get_last_observed_map(engine)

    insert_sql = text(
        """
        INSERT INTO price_demand_observations (sku, observed_at, price, demand)
        VALUES (:sku, :observed_at, :price, :demand)
        ON CONFLICT DO NOTHING
        """
    )

    inserted = 0
    with engine.begin() as conn:
        for sku, observed_at, demand, price in rows:
            if price is None or float(price) <= 0:
                continue
            prev = last_obs.get(sku)
            if prev is not None and observed_at is not None and observed_at <= prev:
                continue
            try:
                conn.execute(insert_sql, {
                    "sku": sku,
                    "observed_at": observed_at,
                    "price": float(price),
                    "demand": int(demand)
                })
                inserted += 1
            except Exception as e:
                logger.error("Failed to insert observation for %s: %s", sku, e)
    return inserted


def compute_elasticity(engine: Engine, sku: str, lookback_hours: int = 72, min_elasticity_observations: int = 5) -> Tuple[Optional[float], int]:
    # Fetch recent observations
    q = text(
        """
        SELECT price, demand
        FROM price_demand_observations
        WHERE sku = :sku AND observed_at >= (CURRENT_TIMESTAMP - INTERVAL '1 hour' * :h)
        ORDER BY observed_at DESC
        LIMIT 200
        """
    )
    with engine.connect() as conn:
        price_demand_observations = conn.execute(q, {"sku": sku, "h": lookback_hours}).all()

    if len(price_demand_observations) < min_elasticity_observations:
        return None, len(price_demand_observations)

    prices = np.array([float(r[0]) for r in price_demand_observations if float(r[0]) > 0])
    demands = np.array([int(r[1]) for r in price_demand_observations if int(r[1]) > 0])
    obs_count = int(min(len(prices), len(demands)))
    if obs_count < min_elasticity_observations:
        return None, obs_count

    # Use natural logs for log-linear model: ln(d) = a + b ln(p)
    ln_p = np.log(prices)
    ln_d = np.log(demands)
    if np.isclose(np.var(ln_p), 0.0):
        return None, obs_count
    # slope b via least squares
    b, a = np.polyfit(ln_p, ln_d, 1)
    # Elasticity is slope b
    return float(b), obs_count


def update_sensitivity_from_elasticity(engine: Engine, logger: logging.Logger, min_elasticity_observations: int = 5) -> None:
    # Get all SKUs
    with engine.connect() as conn:
        skus = [r[0] for r in conn.execute(text("SELECT sku FROM platform_products WHERE is_active = TRUE")).all()]
    if not skus:
        return

    high_thr = float(get_env("ELASTICITY_HIGH_THRESHOLD", "-1.0"))
    med_thr = float(get_env("ELASTICITY_MEDIUM_THRESHOLD", "-0.3"))

    updates: List[Tuple[str, str, float, int]] = []
    for sku in skus:
        e, n_obs = compute_elasticity(engine, sku, min_elasticity_observations=min_elasticity_observations)
        if e is None:
            continue
        # More negative elasticity => more sensitive
        if e <= high_thr:
            sens = 'high'
        elif e <= med_thr:
            sens = 'medium'
        else:
            sens = 'low'
        updates.append((sku, sens, float(e), int(n_obs)))

    if not updates:
        return

    # Bulk update
    with engine.begin() as conn:
        sql = text(
            """
            UPDATE platform_products
            SET price_sensitivity = :sens,
                price_elasticity = :elasticity,
                sensitivity_last_updated = CURRENT_TIMESTAMP,
                sensitivity_observations = :n_obs
            WHERE sku = :sku
            """
        )
        insert_hist = text(
            """
            INSERT INTO price_sensitivity_history
                (sku, price_elasticity, price_sensitivity, observations, updated_at)
            VALUES (:sku, :elasticity, :sens, :n_obs, CURRENT_TIMESTAMP)
            """
        )
        for sku, sens, elasticity, n_obs in updates:
            try:
                conn.execute(sql, {
                    "sens": sens,
                    "elasticity": elasticity,
                    "n_obs": n_obs,
                    "sku": sku,
                })
                conn.execute(insert_hist, {
                    "sku": sku,
                    "elasticity": elasticity,
                    "sens": sens,
                    "n_obs": n_obs
                })
            except Exception as e:
                logger.error("Failed to update sensitivity for %s: %s", sku, e)


def main() -> None:
    logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    logger = logging.getLogger("price_sensitivity_updater")

    refresh_seconds = int(get_env("PRICE_SENSITIVITY_REFRESH_SEC", "60"))
    logger.info("Starting price sensitivity updater. Interval: %ss", refresh_seconds)
    safe_dsn = build_pg_dsn().replace(get_env("POSTGRES_PASSWORD"), "***")
    logger.info("Postgres DSN: %s", safe_dsn)

    engine = create_db_engine()
    ensure_tables_exist(engine)
    rollup_minutes = int(get_env("ELASTICITY_ROLLUP_MINUTES", "60"))
    min_elasticity_observations = int(get_env("MIN_ELASTICITY_OBSERVATIONS", "20"))

    while True:
        try:
            # 1) Collect observations per SKU from DB rollup windows
            inserted = collect_and_store_observations(engine, logger, rollup_minutes=rollup_minutes)
            logger.info("Collected %s observations (rollup=%sm)", inserted, rollup_minutes)
            # 2) Update sensitivity from elasticity where possible
            update_sensitivity_from_elasticity(engine, logger, min_elasticity_observations=min_elasticity_observations)
            # 3) (No rule-based fallback) Elasticity-only updates
        except Exception as e:
            logger.error("Update cycle error: %s", e)
        time.sleep(refresh_seconds)


if __name__ == "__main__":
    main()


