import numpy as np

def candidate_grid(current_price: float, comp_min: float | None, tick: float = 0.10) -> list[float]:
    prices = set()
    if current_price and current_price > 0:
        for r in np.linspace(0.9, 1.12, 15):
            prices.add(round(np.round((current_price * r) / tick) * tick, 2))
    if comp_min and comp_min > 0:
        for r in np.linspace(0.97, 1.08, 12):
            prices.add(round(np.round((comp_min * r) / tick) * tick, 2))
    return sorted(p for p in prices if p > 0)

def choose_price(
    candidates: list[float],
    predict_units,
    unit_cost: float,
    prev_price: float,
    floor: float | None = None,
    ceiling: float | None = None,
    lam_smooth: float = 0.02,
    move_cap: float = 0.15,
):
    best_p, best_profit = None, -1e18
    for p in candidates:
        if floor is not None and p < floor:      # margin / MAP floors
            continue
        if ceiling is not None and p > ceiling:  # brand ceilings etc.
            continue
        if prev_price and abs(p - prev_price)/max(prev_price, 1e-9) > move_cap:  # rate-limit
            continue
        d_hat = max(float(predict_units(p)), 0.0)
        profit = (p - unit_cost) * d_hat - lam_smooth * abs(p - prev_price)
        if profit > best_profit:
            best_p, best_profit = p, profit
    return best_p if best_p is not None else prev_price
