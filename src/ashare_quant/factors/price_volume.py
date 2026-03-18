from __future__ import annotations
import pandas as pd

def add_basic_factors(df: pd.DataFrame) -> pd.DataFrame:
    out = df.sort_values(["ts_code", "trade_date"]).copy()
    g = out.groupby("ts_code", group_keys=False)
    out["ret_5"] = g["close"].pct_change(5)
    out["ret_20"] = g["close"].pct_change(20)
    out["ma20"] = g["close"].transform(lambda x: x.rolling(20).mean())
    out["ma60"] = g["close"].transform(lambda x: x.rolling(60).mean())
    out["vol_ratio_20"] = out["volume"] / g["volume"].transform(lambda x: x.rolling(20).mean())
    return out
