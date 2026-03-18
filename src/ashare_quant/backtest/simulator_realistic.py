from __future__ import annotations
import pandas as pd
from ashare_quant.config.settings import SETTINGS


def _can_buy(row: pd.Series) -> bool:
    if bool(row.get("is_suspended", False)):
        return False
    return float(row["close"]) < float(row.get("up_limit", row["close"] * 10))


def _can_sell(row: pd.Series) -> bool:
    if bool(row.get("is_suspended", False)):
        return False
    return float(row["close"]) > float(row.get("down_limit", row["close"] * -10))


def realistic_topn_backtest(df: pd.DataFrame, top_n: int = 20, hold_days: int = 5) -> pd.DataFrame:
    """Daily rebalance approximation with A-share constraints.

    Important anti-lookahead rule:
    - Ranking uses day t information
    - PnL for holdings is realized with forward return from t to t+1 (fwd_ret_1)
    """
    data = df.sort_values(["ts_code", "trade_date"]).copy()
    data["fwd_ret_1"] = data.groupby("ts_code")["close"].shift(-1) / data["close"] - 1
    data = data.sort_values(["trade_date", "rule_score"], ascending=[True, False]).copy()
    dates = sorted(data["trade_date"].unique().tolist())

    holdings: dict[str, dict] = {}
    equity = 1.0
    curve = []

    for d in dates:
        day = data[data["trade_date"] == d]

        # 1) mark-to-market using forward 1-day return (t -> t+1) on existing holdings
        if holdings:
            hday = day.set_index("ts_code")
            rets = []
            for code in list(holdings.keys()):
                if code in hday.index:
                    fr = hday.loc[code, "fwd_ret_1"]
                    rets.append(float(fr) if pd.notna(fr) else 0.0)
            daily_ret = sum(rets) / len(rets) if rets else 0.0
        else:
            daily_ret = 0.0

        # 2) aging and sell decisions (T+1 by requiring age >=1 before eligible)
        turnover_sell = 0
        for code in list(holdings.keys()):
            holdings[code]["age"] += 1
            if holdings[code]["age"] >= hold_days:
                row = day[day["ts_code"] == code]
                if not row.empty and holdings[code]["age"] >= 1 and _can_sell(row.iloc[0]):
                    del holdings[code]
                    turnover_sell += 1

        # 3) buy topN candidates
        slots = max(0, top_n - len(holdings))
        turnover_buy = 0
        if slots > 0:
            for _, r in day.iterrows():
                code = r["ts_code"]
                if code in holdings:
                    continue
                if not _can_buy(r):
                    continue
                holdings[code] = {"age": 0}
                slots -= 1
                turnover_buy += 1
                if slots <= 0:
                    break

        gross = daily_ret
        trade_frac = (turnover_sell + turnover_buy) / max(top_n, 1)
        cost = trade_frac * (SETTINGS.costs.commission_bps + SETTINGS.costs.slippage_bps) / 10000.0
        tax = (turnover_sell / max(top_n, 1)) * SETTINGS.costs.stamp_duty_sell_bps / 10000.0
        net = gross - cost - tax

        equity *= (1 + net)
        curve.append(
            {
                "trade_date": d,
                "daily_ret": net,
                "equity": equity,
                "n_holdings": len(holdings),
                "turnover_buy": turnover_buy,
                "turnover_sell": turnover_sell,
            }
        )

    return pd.DataFrame(curve)
