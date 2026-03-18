from __future__ import annotations

from pathlib import Path
import json
import numpy as np
import pandas as pd

from ashare_quant.universe.filtering import apply_universe_filters
from ashare_quant.factors.price_volume import add_basic_factors
from ashare_quant.reporting.metrics import evaluate_curve
from ashare_quant.utils.ranking import cs_rank


def harmonize_by_vendor_ratio(df: pd.DataFrame, base_vendor: str = "tushare_daily") -> pd.DataFrame:
    """Simple harmonization: per-symbol median close ratio to base vendor; scale non-base closes.
    This is a pragmatic bridge until corporate-action exact alignment is implemented.
    """
    x = df.copy()
    piv = x.pivot_table(index=["trade_date", "ts_code"], columns="data_vendor", values="close", aggfunc="first")
    vendors = [c for c in piv.columns if c != base_vendor]
    ratio_map = {}
    for v in vendors:
        z = piv[[base_vendor, v]].dropna()
        if z.empty:
            continue
        ratio = (z[base_vendor] / z[v].replace(0, np.nan)).dropna()
        if len(ratio):
            ratio_map[v] = float(ratio.median())

    # apply scale on close/open/high/low for non-base vendors
    for v, s in ratio_map.items():
        m = x["data_vendor"] == v
        for c in ["open", "high", "low", "close", "up_limit", "down_limit"]:
            if c in x.columns:
                x.loc[m, c] = x.loc[m, c] * s
    return x


def merge_priority(df: pd.DataFrame) -> pd.DataFrame:
    rank = {"tushare_daily": 0, "eastmoney_hist": 1, "sina_daily": 2}
    x = df.copy()
    x["_r"] = x["data_vendor"].map(lambda s: rank.get(str(s), 99))
    x = x.sort_values(["trade_date", "ts_code", "_r"]).drop_duplicates(["trade_date", "ts_code"], keep="first")
    return x.drop(columns=["_r"]).reset_index(drop=True)


def make_score(df: pd.DataFrame, family: str, p: dict) -> pd.DataFrame:
    x = df.copy()
    if family == "trend":
        x["rule_score"] = (
            (x["ma20"] > x["ma60"]).astype(int) * p["w_trend"]
            + cs_rank(x, "ret_20") * p["w_mom"]
            + cs_rank(x, "amount") * p["w_liq"]
        )
    elif family == "momentum":
        x["rule_score"] = cs_rank(x, "ret_20") * p["w_20"] + cs_rank(x, "ret_5") * p["w_5"]
    elif family == "ma_cross":
        ma_ratio = (x["ma20"] / x["ma60"]).replace([np.inf, -np.inf], np.nan).fillna(0)
        x["rule_score"] = (x["ma20"] > x["ma60"]).astype(int) * p["w_cross"] + cs_rank(pd.DataFrame({"trade_date": x["trade_date"], "_tmp": ma_ratio}), "_tmp") * p["w_ratio"]
    elif family == "breakout":
        g = x.groupby("ts_code", group_keys=False)
        x["hh_n"] = g["high"].transform(lambda s: s.shift(1).rolling(p["lookback"]).max())
        x["rule_score"] = (x["close"] >= x["hh_n"]).fillna(False).astype(int) * p["w_break"] + cs_rank(x, "vol_ratio_20") * p["w_vol"]
    else:
        raise ValueError(family)
    return x


def run_bt(df: pd.DataFrame, start: str, end: str, top_n: int = 8, hold_days: int = 5) -> pd.DataFrame:
    x = df[(df["trade_date"] >= start) & (df["trade_date"] <= end)].copy()
    x = x.sort_values(["ts_code", "trade_date"]).copy()
    x["fwd_ret_1"] = x.groupby("ts_code")["close"].shift(-1) / x["close"] - 1
    dates = sorted(x["trade_date"].unique().tolist())
    h, eq, rows = {}, 1.0, []
    for d in dates:
        day = x[x["trade_date"] == d].sort_values("rule_score", ascending=False)
        if h:
            k = day.set_index("ts_code")
            rr = [float(k.loc[c, "fwd_ret_1"]) for c in h if c in k.index and pd.notna(k.loc[c, "fwd_ret_1"])]
            dr = float(np.mean(rr)) if rr else 0.0
        else:
            dr = 0.0
        sell = 0
        for c in list(h.keys()):
            h[c] += 1
            if h[c] >= hold_days:
                del h[c]
                sell += 1
        buy = 0
        slots = max(0, top_n - len(h))
        if slots > 0:
            for _, r in day.iterrows():
                c = r["ts_code"]
                if c in h:
                    continue
                h[c] = 0
                buy += 1
                slots -= 1
                if slots <= 0:
                    break
        trade_frac = (buy + sell) / max(top_n, 1)
        net = dr - trade_frac * (2.5 + 5.0) / 10000 - (sell / max(top_n, 1)) * 10.0 / 10000
        eq *= (1 + net)
        rows.append({"trade_date": d, "daily_ret": net, "equity": eq})
    return pd.DataFrame(rows)


def latest_picks(df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    d = df["trade_date"].max()
    day = df[df["trade_date"] == d].sort_values("rule_score", ascending=False).copy()
    picks = day[["trade_date", "ts_code", "close", "rule_score", "ret_20", "amount"]].head(top_n)
    picks["target_weight"] = 1.0 / max(len(picks), 1)
    return picks


def run() -> None:
    p = Path("data/market_daily_multivendor_cleaned.parquet")
    if not p.exists():
        raise FileNotFoundError(p)
    raw = pd.read_parquet(p)
    hz = harmonize_by_vendor_ratio(raw, base_vendor="tushare_daily")
    merged = merge_priority(hz)
    merged = apply_universe_filters(merged)
    merged = add_basic_factors(merged)

    candidates = [
        ("trend", {"w_trend": 0.4, "w_mom": 0.4, "w_liq": 0.2}),
        ("momentum", {"w_20": 0.7, "w_5": 0.3}),
        ("trend", {"w_trend": 0.5, "w_mom": 0.3, "w_liq": 0.2}),
        ("ma_cross", {"w_cross": 0.7, "w_ratio": 0.3}),
        ("breakout", {"lookback": 20, "w_break": 0.7, "w_vol": 0.3}),
        ("breakout", {"lookback": 55, "w_break": 0.7, "w_vol": 0.3}),
    ]
    windows = [
        ("wf1", "2019-01-02", "2021-12-31", "2022-01-01", "2023-12-31"),
        ("wf2", "2020-01-01", "2022-12-31", "2023-01-01", "2024-12-31"),
        ("wf3", "2021-01-01", "2023-12-31", "2024-01-01", "2026-03-13"),
    ]

    rows = []
    scored_cache = {}
    for fam, par in candidates:
        key = (fam, json.dumps(par, sort_keys=True, ensure_ascii=False))
        sx = make_score(merged, fam, par)
        scored_cache[key] = sx
        t_sharpes, t_mdds, t_anns = [], [], []
        for wn, tr_s, tr_e, te_s, te_e in windows:
            c_tr = run_bt(sx, tr_s, tr_e)
            c_te = run_bt(sx, te_s, te_e)
            mtr, mts = evaluate_curve(c_tr), evaluate_curve(c_te)
            rows.append({"family": fam, "params": par, "window": wn, "train": mtr, "test": mts})
            t_sharpes.append(mts["sharpe"]) ; t_mdds.append(mts["max_drawdown"]) ; t_anns.append(mts["ann_return"])
        rows.append({
            "family": fam,
            "params": par,
            "window": "aggregate",
            "test": {
                "sharpe_mean": float(np.mean(t_sharpes)),
                "sharpe_min": float(np.min(t_sharpes)),
                "ann_mean": float(np.mean(t_anns)),
                "mdd_worst": float(np.min(t_mdds)),
            },
        })

    # aggregate ranking
    aggs = [r for r in rows if r["window"] == "aggregate"]
    aggs = sorted(aggs, key=lambda r: (r["test"]["sharpe_mean"], r["test"]["ann_mean"], -abs(r["test"]["mdd_worst"])), reverse=True)

    kept, eliminated = [], []
    for r in aggs:
        t = r["test"]
        reasons = []
        if t["sharpe_mean"] < 0.35: reasons.append("wf_sharpe_mean<0.35")
        if t["sharpe_min"] < 0.10: reasons.append("wf_sharpe_min<0.10")
        if t["mdd_worst"] < -0.40: reasons.append("wf_worst_mdd<-40%")
        if t["ann_mean"] <= 0: reasons.append("wf_ann_mean<=0")
        if reasons: eliminated.append({"family": r["family"], "params": r["params"], "agg": t, "reasons": reasons})
        else: kept.append(r)

    # final execution picks from best kept strategy on latest date
    best = kept[0] if kept else aggs[0]
    key = (best["family"], json.dumps(best["params"], sort_keys=True, ensure_ascii=False))
    picks = latest_picks(scored_cache[key], top_n=5)

    out = Path("reports")
    out.mkdir(parents=True, exist_ok=True)
    (out / "walkforward_results_v2.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "strategy_candidate_list_v2.json").write_text(json.dumps(kept, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "strategy_elimination_list_v2.json").write_text(json.dumps(eliminated, ensure_ascii=False, indent=2), encoding="utf-8")
    picks.to_csv(out / "daily_picks_v2.csv", index=False)

    summary = {
        "best_family": best["family"],
        "best_params": best["params"],
        "best_agg": best["test"],
        "kept": len(kept),
        "eliminated": len(eliminated),
        "latest_trade_date": str(picks["trade_date"].iloc[0]) if len(picks) else None,
        "n_latest_picks": int(len(picks)),
    }
    (out / "execution_summary_v2.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(picks.to_string(index=False))


if __name__ == "__main__":
    run()
