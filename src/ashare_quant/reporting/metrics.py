from __future__ import annotations
import numpy as np
import pandas as pd


def evaluate_curve(curve: pd.DataFrame) -> dict:
    r = curve["daily_ret"].fillna(0.0)
    eq = curve["equity"].ffill().fillna(1.0)
    ann_ret = float(eq.iloc[-1] ** (252 / max(len(eq), 1)) - 1)
    ann_vol = float(r.std(ddof=0) * np.sqrt(252))
    sharpe = float(ann_ret / ann_vol) if ann_vol > 1e-12 else 0.0
    mdd = float((eq / eq.cummax() - 1).min())
    win_rate = float((r > 0).mean())
    return {
        "ann_return": ann_ret,
        "ann_vol": ann_vol,
        "sharpe": sharpe,
        "max_drawdown": mdd,
        "win_rate": win_rate,
        "days": int(len(curve)),
    }
