from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
LAKE = ROOT / "quant-next" / "data" / "lake"

bars_p = LAKE / "daily_bars.parquet"
val_p = LAKE / "valuation_daily.parquet"

if not bars_p.exists() or not val_p.exists():
    raise FileNotFoundError("Run adapter first")

bars = pd.read_parquet(bars_p)
val = pd.read_parquet(val_p)

print("== daily_bars ==")
print({
    "rows": int(len(bars)),
    "symbols": int(bars["ts_code"].astype(str).nunique()),
    "date_min": str(pd.to_datetime(bars["trade_date"]).min().date()),
    "date_max": str(pd.to_datetime(bars["trade_date"]).max().date()),
    "null_ratio": bars.isna().mean().sort_values(ascending=False).head(8).to_dict(),
})

print("== valuation_daily ==")
print({
    "rows": int(len(val)),
    "symbols": int(val["ts_code"].astype(str).nunique()),
    "date_min": str(pd.to_datetime(val["trade_date"]).min().date()),
    "date_max": str(pd.to_datetime(val["trade_date"]).max().date()),
    "null_ratio": val.isna().mean().sort_values(ascending=False).head(8).to_dict(),
})

common = set(bars["ts_code"].astype(str).unique()) & set(val["ts_code"].astype(str).unique())
print({"symbol_overlap": len(common)})
