from __future__ import annotations
from pathlib import Path
import pandas as pd

def write_daily_candidates(df: pd.DataFrame, trade_date: str, out_path: str = "reports/daily_candidates.csv") -> Path:
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    out = df[df["trade_date"] == trade_date].sort_values("rule_score", ascending=False)
    out[["trade_date", "ts_code", "rule_score", "ret_20", "amount"]].head(50).to_csv(p, index=False)
    return p
