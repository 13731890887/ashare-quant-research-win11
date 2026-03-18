from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]  # ashare-quant-research-win11
OLD = ROOT
OUT = ROOT / "quant-next" / "data" / "lake"
OUT.mkdir(parents=True, exist_ok=True)

bars_p = OLD / "data" / "stage4_all_buyable" / "market_daily_all_buyable_20210101_20260314.parquet"
pe_p = OLD / "data" / "fundamental_cache" / "pe_ttm.parquet"
pb_p = OLD / "data" / "fundamental_cache" / "pb.parquet"
mv_p = OLD / "data" / "fundamental_cache" / "mv_total.parquet"

if not bars_p.exists():
    raise FileNotFoundError(bars_p)

bars = pd.read_parquet(bars_p)
bars["trade_date"] = pd.to_datetime(bars["trade_date"]).dt.date
bars["ts_code"] = bars["ts_code"].astype(str).str.zfill(6)

need_bars = [
    "trade_date","ts_code","open","high","low","close","volume","amount",
    "is_st","is_suspended","up_limit","down_limit","data_vendor"
]
for c in need_bars:
    if c not in bars.columns:
        bars[c] = None
bars = bars[need_bars].copy()
bars.to_parquet(OUT / "daily_bars.parquet", index=False)

# valuation merge
for p, col in [(pe_p, "pe_ttm"), (pb_p, "pb"), (mv_p, "mv_total")]:
    if not p.exists():
        raise FileNotFoundError(p)

pe = pd.read_parquet(pe_p).rename(columns={"value": "pe_ttm"})[["trade_date", "ts_code", "pe_ttm"]]
pb = pd.read_parquet(pb_p).rename(columns={"value": "pb"})[["trade_date", "ts_code", "pb"]]
mv = pd.read_parquet(mv_p).rename(columns={"value": "mv_total"})[["trade_date", "ts_code", "mv_total"]]

for df in (pe, pb, mv):
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["ts_code"] = df["ts_code"].astype(str).str.zfill(6)

val = pe.merge(pb, on=["trade_date", "ts_code"], how="outer")\
        .merge(mv, on=["trade_date", "ts_code"], how="outer")
val = val.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)
val.to_parquet(OUT / "valuation_daily.parquet", index=False)

print({
    "daily_bars_rows": int(len(bars)),
    "daily_bars_symbols": int(bars["ts_code"].nunique()),
    "valuation_rows": int(len(val)),
    "valuation_symbols": int(val["ts_code"].nunique()),
    "out": str(OUT),
})
