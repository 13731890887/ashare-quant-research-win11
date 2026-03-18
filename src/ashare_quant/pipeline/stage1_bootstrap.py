from __future__ import annotations
import numpy as np
import pandas as pd
from ashare_quant.factors.price_volume import add_basic_factors
from ashare_quant.scoring.rules import score_transparent_rules
from ashare_quant.universe.filtering import apply_universe_filters
from ashare_quant.backtest.simulator import simple_topn_backtest
from ashare_quant.experiments.tracker import log_experiment


def build_mock_data(days: int = 260, n_stocks: int = 300) -> pd.DataFrame:
    dates = pd.bdate_range(end=pd.Timestamp.today().normalize(), periods=days)
    codes = [f"{i:06d}.SZ" for i in range(1, n_stocks + 1)]
    recs = []
    rng = np.random.default_rng(42)
    for c in codes:
        px = 10 + rng.random() * 40
        for d in dates:
            drift = rng.normal(0.0003, 0.02)
            px = max(1, px * (1 + drift))
            vol = int(rng.integers(300_000, 10_000_000))
            amt = px * vol
            recs.append([d.strftime("%Y-%m-%d"), c, px*0.99, px*1.01, px*0.98, px, vol, amt, False, False, px*1.1, px*0.9])
    return pd.DataFrame(recs, columns=["trade_date","ts_code","open","high","low","close","volume","amount","is_st","is_suspended","up_limit","down_limit"])


def run() -> None:
    df = build_mock_data()
    df = apply_universe_filters(df)
    df = add_basic_factors(df)
    df = score_transparent_rules(df)
    bt = simple_topn_backtest(df, top_n=20)
    ann = float((bt["equity"].iloc[-1] ** (252 / max(len(bt), 1))) - 1)
    mdd = float((bt["equity"] / bt["equity"].cummax() - 1).min())
    log_experiment("stage1_mock_baseline", {"top_n": 20}, {"ann_return": ann, "max_drawdown": mdd})
    print({"ann_return": round(ann, 4), "max_drawdown": round(mdd, 4), "rows": len(df)})

if __name__ == "__main__":
    run()
