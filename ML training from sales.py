from pathlib import Path
import pandas as pd
import numpy as np

SRC = Path("sales_modified.csv") if Path("sales_modified.csv").exists() else Path("sales.csv")
OUT_DIR = Path("data"); OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT = OUT_DIR / "training_table.csv"

def main():
    if not SRC.exists():
        raise FileNotFoundError(f"Could not find {SRC}. Put sales.csv at repo root (or sales_modified.csv).")

    usecols = [
        "product_id","store_id","date","sales","stock","price",
        "promo_type_1","promo_bin_1","promo_type_2","promo_bin_2","category","brand"
    ]
    df = pd.read_csv(SRC, sep=",", usecols=[c for c in usecols if c in pd.read_csv(SRC, nrows=0).columns])

    # Clean and derive
    df["date"] = pd.to_datetime(df["date"])
    df["units_sold"] = pd.to_numeric(df["sales"], errors="coerce").fillna(0.0)
    df["our_price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0.0)
    df["stock_level"] = pd.to_numeric(df["stock"], errors="coerce").fillna(0.0)

    # promotions_active (binary proxy)
    promo_cols = [c for c in ["promo_type_1","promo_bin_1","promo_type_2","promo_bin_2"] if c in df.columns]
    df["promotions_active"] = (df[promo_cols].notna().sum(axis=1) > 0).astype(int) if promo_cols else 0

    # time features
    df["dow"]  = df["date"].dt.weekday
    df["hour"] = 12  # daily data → use a neutral hour
    df["month"] = df["date"].dt.month

    # competitor & traffic placeholders (not in sales.csv)
    df["comp_min_price"]  = 0.0
    df["comp_mean_price"] = 0.0
    df["comp_instock"]    = 0
    df["views"]           = (df["units_sold"] * 30).astype(int)  # crude proxy so model learns monotonic demand
    df["add_to_cart"]     = (df["units_sold"] * 10).astype(int)

    # final select/rename
    final = df.rename(columns={"product_id":"product_id"})[[
        "product_id","date","our_price","stock_level","promotions_active",
        "dow","hour","views","add_to_cart",
        "comp_min_price","comp_mean_price","comp_instock",
        "units_sold"
    ]]

    final.to_csv(OUT, index=False)
    print("Wrote", OUT, "rows:", len(final))

if __name__ == "__main__":
    main()
