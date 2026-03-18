from __future__ import annotations

from pathlib import Path
import json
import itertools
import numpy as np
import pandas as pd

from ashare_quant.universe.filtering import apply_universe_filters
from ashare_quant.factors.price_volume import add_basic_factors
from ashare_quant.reporting.metrics import evaluate_curve
from ashare_quant.utils.ranking import cs_rank


def make_scores(df: pd.DataFrame, family: str, p: dict) -> pd.DataFrame:
    x = df.copy()
    if family == "trend":
        x["score"] = (
            (x["ma20"] > x["ma60"]).astype(int) * p["w_trend"]
            + cs_rank(x, "ret_20") * p["w_mom"]
            + (cs_rank(x, "amount")) * p["w_liq"]
        )
    elif family == "momentum":
        x["score"] = cs_rank(x, "ret_20") * p["w_20"] + cs_rank(x, "ret_5") * p["w_5"]
    elif family == "ma_cross":
        # emphasize distance above moving average as strength
        ma_ratio = (x["ma20"] / x["ma60"]).replace([np.inf, -np.inf], np.nan).fillna(0)
        x["score"] = (x["ma20"] > x["ma60"]).astype(int) * p["w_cross"] + cs_rank(pd.DataFrame({"trade_date": x["trade_date"], "_tmp": ma_ratio}), "_tmp") * p["w_ratio"]
    elif family == "breakout":
        g = x.groupby("ts_code", group_keys=False)
        x["hh_n"] = g["high"].transform(lambda s: s.shift(1).rolling(p["lookback"]).max())
        br = (x["close"] >= x["hh_n"]).fillna(False).astype(int)
        x["score"] = br * p["w_break"] + cs_rank(x, "vol_ratio_20") * p["w_vol"]
    else:
        raise ValueError(f"unknown family {family}")
    return x


def run_backtest(df: pd.DataFrame, score_col: str, top_n: int, hold_days: int, start: str, end: str) -> pd.DataFrame:
    x = df[(df["trade_date"] >= start) & (df["trade_date"] <= end)].copy()
    x = x.sort_values(["ts_code", "trade_date"]).copy()
    x["fwd_ret_1"] = x.groupby("ts_code")["close"].shift(-1) / x["close"] - 1

    dates = sorted(x["trade_date"].unique().tolist())
    holdings = {}
    eq = 1.0
    rows = []

    for d in dates:
        day = x[x["trade_date"] == d].sort_values(score_col, ascending=False)
        # pnl
        if holdings:
            h = day.set_index("ts_code")
            r = [float(h.loc[c, "fwd_ret_1"]) for c in holdings.keys() if c in h.index and pd.notna(h.loc[c, "fwd_ret_1"]) ]
            dr = float(np.mean(r)) if r else 0.0
        else:
            dr = 0.0

        # age and sell
        sell = 0
        for c in list(holdings.keys()):
            holdings[c] += 1
            if holdings[c] >= hold_days:
                del holdings[c]
                sell += 1

        # buy
        buy = 0
        slots = max(0, top_n - len(holdings))
        if slots > 0:
            for _, r0 in day.iterrows():
                c = r0["ts_code"]
                if c in holdings:
                    continue
                holdings[c] = 0
                buy += 1
                slots -= 1
                if slots <= 0:
                    break

        # simple cost model
        trade_frac = (buy + sell) / max(top_n, 1)
        cost = trade_frac * (2.5 + 5.0) / 10000.0
        tax = (sell / max(top_n, 1)) * 10.0 / 10000.0
        net = dr - cost - tax

        eq *= (1 + net)
        rows.append({"trade_date": d, "daily_ret": net, "equity": eq})

    return pd.DataFrame(rows)


def run() -> None:
    src = Path("data/market_daily_research_ready.parquet")
    if not src.exists():
        raise FileNotFoundError(src)
    df = pd.read_parquet(src)
    df = apply_universe_filters(df)
    df = add_basic_factors(df)

    train = ("2019-01-02", "2023-12-31")
    test = ("2024-01-01", "2026-03-13")

    grids = {
        "trend": [
            {"w_trend": 0.5, "w_mom": 0.3, "w_liq": 0.2},
            {"w_trend": 0.4, "w_mom": 0.4, "w_liq": 0.2},
        ],
        "momentum": [
            {"w_20": 0.7, "w_5": 0.3},
            {"w_20": 0.5, "w_5": 0.5},
        ],
        "ma_cross": [
            {"w_cross": 0.7, "w_ratio": 0.3},
            {"w_cross": 0.5, "w_ratio": 0.5},
        ],
        "breakout": [
            {"lookback": 20, "w_break": 0.7, "w_vol": 0.3},
            {"lookback": 55, "w_break": 0.7, "w_vol": 0.3},
        ],
    }

    results = []
    for family, plist in grids.items():
        for p in plist:
            sx = make_scores(df, family, p)
            sx = sx.rename(columns={"score": "rule_score"})
            c_train = run_backtest(sx, "rule_score", top_n=8, hold_days=5, start=train[0], end=train[1])
            c_test = run_backtest(sx, "rule_score", top_n=8, hold_days=5, start=test[0], end=test[1])
            mtr = evaluate_curve(c_train)
            mts = evaluate_curve(c_test)
            results.append({
                "family": family,
                "params": p,
                "train": mtr,
                "test": mts,
            })

    # ranking: prioritize out-of-sample sharpe, then drawdown, then ann return
    ranked = sorted(
        results,
        key=lambda r: (r["test"]["sharpe"], r["test"]["ann_return"], -abs(r["test"]["max_drawdown"])),
        reverse=True,
    )

    # eliminate rules
    eliminated, kept = [], []
    for r in ranked:
        t = r["test"]
        reason = []
        if t["sharpe"] < 0.3:
            reason.append("test_sharpe<0.3")
        if t["max_drawdown"] < -0.45:
            reason.append("test_mdd<-45%")
        if t["ann_return"] <= 0:
            reason.append("test_ann_return<=0")
        if reason:
            eliminated.append({"family": r["family"], "params": r["params"], "test": t, "reasons": reason})
        else:
            kept.append(r)

    out_dir = Path("reports")
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "strategy_family_results_v1.json").write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "strategy_elimination_list_v1.json").write_text(json.dumps(eliminated, ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "strategy_candidate_list_v1.json").write_text(json.dumps(kept[:3], ensure_ascii=False, indent=2), encoding="utf-8")

    print("top3_by_test:")
    for r in ranked[:3]:
        print(r["family"], r["params"], r["test"])
    print({"kept": len(kept), "eliminated": len(eliminated)})


if __name__ == "__main__":
    run()
