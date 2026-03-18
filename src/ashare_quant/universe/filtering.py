from __future__ import annotations
import pandas as pd
from ashare_quant.config.settings import SETTINGS

def apply_universe_filters(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if SETTINGS.universe.exclude_st and "is_st" in out.columns:
        out = out[~out["is_st"].fillna(False)]
    if SETTINGS.universe.exclude_suspended and "is_suspended" in out.columns:
        out = out[~out["is_suspended"].fillna(False)]
    if "amount" in out.columns:
        out = out[out["amount"] >= SETTINGS.universe.min_turnover_million * 1_000_000]
    return out
