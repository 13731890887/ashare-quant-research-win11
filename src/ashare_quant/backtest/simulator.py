from __future__ import annotations
import pandas as pd

def simple_topn_backtest(df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    data = df.sort_values(["trade_date", "rule_score"], ascending=[True, False]).copy()
    picks = data.groupby("trade_date").head(top_n)
    daily_ret = picks.groupby("trade_date")["ret_5"].mean().fillna(0) / 5
    equity = (1 + daily_ret).cumprod()
    return pd.DataFrame({"trade_date": daily_ret.index, "daily_ret": daily_ret.values, "equity": equity.values})
