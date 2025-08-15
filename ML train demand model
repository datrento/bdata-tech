from pathlib import Path
import pandas as pd, numpy as np, xgboost as xgb, json

DATA = Path("data/training_table.csv")
OUT  = Path("ml/artifacts"); OUT.mkdir(parents=True, exist_ok=True)

FEATURES = [
    "our_price","comp_min_price","comp_mean_price","comp_instock",
    "views","add_to_cart","stock_level","promotions_active","dow","hour","rel_to_comp_min"
]

def main():
    if not DATA.exists():
        raise FileNotFoundError("data/training_table.csv not found. Run ml/build_training_from_sales.py first.")

    df = pd.read_csv(DATA)
    df["rel_to_comp_min"] = np.where(df["comp_min_price"]>0, df["our_price"]/df["comp_min_price"], 1.0)
    df = df.replace([np.inf,-np.inf], np.nan).fillna(0.0)

    y = np.log1p(df["units_sold"].astype(float))
    X = df[FEATURES].astype(float)

    dtrain = xgb.DMatrix(X, label=y, feature_names=FEATURES)
    params = dict(objective="reg:squarederror", eta=0.05, max_depth=8,
                  subsample=0.8, colsample_bytree=0.8, eval_metric="rmse")
    bst = xgb.train(params, dtrain, num_boost_round=600)

    bst.save_model(str(OUT/"demand_xgb.json"))
    (OUT/"features.json").write_text(json.dumps(FEATURES))
    print("Saved model to", OUT)

if __name__ == "__main__":
    main()
