from __future__ import annotations
import pandas as pd

def cs_rank(df: pd.DataFrame, col: str, date_col: str = "trade_date") -> pd.Series:
    """Cross-sectional percentile rank by date (anti-time-leakage)."""
    return df.groupby(date_col)[col].rank(pct=True)
