import os
import time
import logging
from typing import Dict, List, Optional, Tuple
import numpy as np
import httpx
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


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


def fetch_user_behavior(api_base: str, sku: str, client: httpx.Client) -> Optional[int]:
    url = f"{api_base}/user-behavior/data/{sku}"
    try:
        # Synchronous wrapper using .get with timeout
        resp = client.get(url, timeout=10.0)
        if resp.status_code != 200:
            return None
        data = resp.json()
        ub = data.get("user_behavior", {})
        purchases = ub.get("purchases")
        if purchases is None:
            return None
        return int(purchases)
    except Exception:
        return None


def collect_and_store_observations(engine: Engine, api_base: str, logger: logging.Logger) -> None:
    # Read SKUs and current prices
    with engine.connect() as conn:
        skus = conn.execute(text("SELECT sku, current_price FROM platform_products WHERE is_active = TRUE")).all()

    if not skus:
        return

    with httpx.Client() as client:
        rows_to_insert: List[Tuple[str, float, int]] = []
        for row in skus:
            sku, price = row[0], float(row[1]) if row[1] is not None else None
            if price is None or price <= 0:
                continue
            purchases = fetch_user_behavior(api_base, sku, client)
            if purchases is None or purchases < 0:
                continue
            rows_to_insert.append((sku, price, purchases))

    if not rows_to_insert:
        return

    # Insert observations
    insert_sql = text(
        """
        INSERT INTO price_demand_observations (sku, price, demand)
        VALUES (:sku, :price, :demand)
        """
    )
    with engine.begin() as conn:
        for sku, price, demand in rows_to_insert:
            try:
                conn.execute(insert_sql, {"sku": sku, "price": price, "demand": int(demand)})
            except Exception as e:
                logger.error("Failed to insert observation for %s: %s", sku, e)


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

    updates: List[Tuple[str, str, float, int]] = []
    for sku in skus:
        e, n_obs = compute_elasticity(engine, sku, min_elasticity_observations=min_elasticity_observations)
        if e is None:
            continue
        # More negative elasticity => more sensitive
        if e <= -1.0:
            sens = 'high'
        elif e <= -0.3:
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
        for sku, sens, elasticity, n_obs in updates:
            try:
                conn.execute(sql, {
                    "sens": sens,
                    "elasticity": elasticity,
                    "n_obs": n_obs,
                    "sku": sku,
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
    api_base = get_env("DATA_API_INTERNAL_URL", "http://market-data-collection-api:8000/api")
    min_elasticity_observations = int(get_env("MIN_ELASTICITY_OBSERVATIONS", "5"))

    while True:
        try:
            # 1) Collect one observation per product (current price, purchases)
            collect_and_store_observations(engine, api_base, logger)
            # 2) Update sensitivity from elasticity where possible
            update_sensitivity_from_elasticity(engine, logger, min_elasticity_observations=min_elasticity_observations)
            # 3) (No rule-based fallback) Elasticity-only updates
        except Exception as e:
            logger.error("Update cycle error: %s", e)
        time.sleep(refresh_seconds)


if __name__ == "__main__":
    main()


