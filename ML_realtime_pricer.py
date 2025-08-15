import os, json, time, math
from datetime import datetime, timedelta
from collections import defaultdict
import numpy as np
import xgboost as xgb
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from price_optimizer import candidate_grid, choose_price

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVER_EXTERNAL", "localhost:9092")
TOPIC_COMP = os.getenv("TOPIC_COMPETITOR", "competitor-prices")
TOPIC_AGG  = os.getenv("TOPIC_AGGREGATOR", "aggregator-prices")
TOPIC_UB   = os.getenv("TOPIC_USER_BEHAVIOR", "user-behavior-events")  # optional
TOPIC_REC  = os.getenv("TOPIC_RECS", "pricing-recommendations")

MODEL_PATH  = os.getenv("MODEL_PATH", "ml/artifacts/demand_xgb.json")
FEATURES_FP = os.getenv("FEATURES_PATH", "ml/artifacts/features.json")

WINDOW_MIN = int(os.getenv("STATE_WINDOW_MIN", "20"))
TICK_SEC   = int(os.getenv("REPRICE_EVERY_SEC", "30"))
MARGIN_FLOOR = float(os.getenv("MARGIN_FLOOR", "0.05"))

BASE_MAP = {
    "IPHONE-15-PRO-128": (999.0, 740.0),
    "MACBOOK-AIR-M2-13": (1199.0, 900.0),
    "PS5-CONSOLE": (499.0, 380.0)
}

def ensure_topic(name: str, partitions=3, rf=1):
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    if name in admin.list_topics(timeout=5).topics: return
    admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=rf)])
    time.sleep(2)

def make_consumer(group="ml-pricer"):
    return Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True
    })

def make_producer():
    return Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 10})

def load_model():
    if not os.path.exists(MODEL_PATH):
        return None, None
    bst = xgb.Booster(); bst.load_model(MODEL_PATH)
    import json
    with open(FEATURES_FP) as f: feats = json.load(f)
    return bst, feats

bst, FEATURES = load_model()

def predict_units_fn(context_row: dict):
    if bst is None or FEATURES is None:
        comp_min = context_row.get("comp_min_price", 0.0)
        base_demand = max(context_row.get("views", 0) * 0.03, 1.0)
        def fallback(price: float):
            rel = (price / comp_min) if comp_min else 1.0
            elasticity = -1.2 * (rel - 1.0)
            return max(base_demand * math.exp(elasticity), 0.0)
        return fallback

    def predict(price: float):
        row = context_row.copy()
        row["our_price"] = price
        cm = row.get("comp_min_price", 0.0)
        row["rel_to_comp_min"] = (price/cm) if cm else 1.0
        x = np.array([[float(row.get(f, 0.0)) for f in FEATURES]])
        dm = xgb.DMatrix(x, feature_names=FEATURES)
        logu = bst.predict(dm)[0]
        return max(np.expm1(logu), 0.0)
    return predict

class ProductState:
    def __init__(self):
        self.comp = {}     # competitor -> (price, in_stock, ts)
        self.ub = {}       # user behavior snapshot
        self.agg = {}      # aggregator snapshot
        self.prev_price = None
        self.last_clean = datetime.utcnow()

    def clean(self):
        now = datetime.utcnow()
        if (now - self.last_clean).seconds < 30: return
        cutoff = now - timedelta(minutes=WINDOW_MIN)
        self.comp = {k:v for k,v in self.comp.items() if v[2] >= cutoff}
        self.last_clean = now

    def comp_stats(self):
        prices = [v[0] for v in self.comp.values() if v[0] is not None]
        instock = [v[1] for v in self.comp.values()]
        comp_min = round(min(prices), 2) if prices else 0.0
        comp_mean = round(sum(prices)/len(prices), 2) if prices else 0.0
        return comp_min, comp_mean, int(sum(1 for s in instock if s))

def main():
    for t in [TOPIC_COMP, TOPIC_AGG, TOPIC_UB, TOPIC_REC]:
        ensure_topic(t)

    c = make_consumer()
    c.subscribe([TOPIC_COMP, TOPIC_AGG, TOPIC_UB])
    p = make_producer()

    state: dict[str, ProductState] = defaultdict(ProductState)
    last_tick = time.time() - TICK_SEC

    try:
        while True:
            msg = c.poll(0.2)
            if msg and not msg.error():
                v = json.loads(msg.value().decode("utf-8"))
                sku = v.get("product_sku") or v.get("product_id") or v.get("sku")
                if not sku:
                    continue
                ps = state[sku]; ps.clean()
                if msg.topic() == TOPIC_COMP:
                    ps.comp[v.get("competitor","?")] = (
                        float(v.get("price", 0.0)),
                        bool(v.get("in_stock", False)),
                        datetime.utcnow()
                    )
                elif msg.topic() == TOPIC_UB:
                    ps.ub = v
                elif msg.topic() == TOPIC_AGG:
                    ps.agg = v

            # pricing tick
            if time.time() - last_tick >= TICK_SEC:
                last_tick = time.time()
                for sku, ps in state.items():
                    ps.clean()
                    comp_min, comp_mean, comp_instock = ps.comp_stats()

                    our_price, unit_cost = BASE_MAP.get(sku, (499.0, 350.0))
                    prev_price = ps.prev_price or our_price
                    views = int(ps.ub.get("views", 0) or ps.ub.get("page_views", 0) or 100)
                    add_to_cart = int(ps.ub.get("add_to_cart", 0) or ps.ub.get("cart_additions", 0) or max(1, views//10))
                    stock_level = int(ps.agg.get("stock_level", 500))
                    promo = int(ps.agg.get("promotions_active", 0))

                    ctx = {
                        "our_price": float(prev_price),
                        "comp_min_price": float(comp_min),
                        "comp_mean_price": float(comp_mean),
                        "comp_instock": int(comp_instock),
                        "views": int(views),
                        "add_to_cart": int(add_to_cart),
                        "stock_level": int(stock_level),
                        "promotions_active": promo,
                        "dow": datetime.now().weekday(),
                        "hour": datetime.now().hour
                    }

                    predict_units = predict_units_fn(ctx)
                    grid = candidate_grid(prev_price, comp_min)
                    floor = max(unit_cost * (1 + MARGIN_FLOOR), 0.01)

                    rec = choose_price(grid, predict_units, unit_cost, prev_price, floor=floor,
                                       ceiling=None, lam_smooth=0.02, move_cap=0.15)
                    ps.prev_price = rec

                    out = {
                        "ts": datetime.utcnow().isoformat()+"Z",
                        "product_id": sku,
                        "recommended_price": float(round(rec,2)),
                        "unit_cost": float(unit_cost),
                        "prev_price": float(prev_price),
                        "comp_min_price": float(comp_min),
                        "comp_mean_price": float(comp_mean),
                        "comp_instock": int(comp_instock),
                        "views": int(views),
                        "add_to_cart": int(add_to_cart),
                        "stock_level": int(stock_level),
                        "explain": {"grid_size": len(grid), "floor": floor, "model": "xgb" if bst else "heuristic"}
                    }
                    p.produce(TOPIC_REC, key=sku, value=json.dumps(out).encode("utf-8"))
                p.flush(1)
    except KeyboardInterrupt:
        pass
    finally:
        c.close()

if __name__ == "__main__":
    main()
