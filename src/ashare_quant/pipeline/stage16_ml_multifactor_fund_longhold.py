from __future__ import annotations

from pathlib import Path
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import akshare as ak
from sklearn.ensemble import RandomForestRegressor
from joblib import Parallel, delayed

from ashare_quant.config.research_config import load_config
from ashare_quant.universe.filtering import apply_universe_filters
from ashare_quant.factors.price_volume import add_basic_factors
from ashare_quant.reporting.metrics import evaluate_curve


def cs_rank(df: pd.DataFrame, col: str) -> pd.Series:
    return df.groupby("trade_date")[col].rank(pct=True)


def fetch_baidu_indicator(symbol: str, indicator: str, retries: int = 2) -> pd.DataFrame:
    for i in range(retries + 1):
        try:
            d = ak.stock_zh_valuation_baidu(symbol=symbol, indicator=indicator, period="全部")
            if d is None or d.empty:
                return pd.DataFrame(columns=["trade_date", "value", "ts_code"])
            x = d.copy()
            x["trade_date"] = pd.to_datetime(x["date"])
            x["value"] = pd.to_numeric(x["value"], errors="coerce")
            x["ts_code"] = symbol
            return x[["trade_date", "value", "ts_code"]].dropna(subset=["value"])
        except Exception:
            if i == retries:
                return pd.DataFrame(columns=["trade_date", "value", "ts_code"])
            time.sleep(0.4 + 0.5 * i)
    return pd.DataFrame(columns=["trade_date", "value", "ts_code"])


def build_valuation_cache(symbols: list[str], indicator: str, out_path: Path, max_workers: int = 8) -> pd.DataFrame:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    existing = pd.DataFrame(columns=["trade_date", "value", "ts_code"])
    done_symbols: set[str] = set()
    if out_path.exists():
        try:
            existing = pd.read_parquet(out_path)
            if not existing.empty:
                existing["ts_code"] = existing["ts_code"].astype(str).str.zfill(6)
                existing["trade_date"] = pd.to_datetime(existing["trade_date"])
                done_symbols = set(existing["ts_code"].unique().tolist())
        except Exception:
            existing = pd.DataFrame(columns=["trade_date", "value", "ts_code"])

    pending = sorted(set(symbols) - done_symbols)
    print({"indicator": indicator, "cached_symbols": len(done_symbols), "pending": len(pending)}, flush=True)

    frames = [existing] if not existing.empty else []
    if pending:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = {ex.submit(fetch_baidu_indicator, s, indicator): s for s in pending}
            total = len(futs)
            for i, f in enumerate(as_completed(futs), 1):
                r = f.result()
                if r is not None and not r.empty:
                    frames.append(r)
                if i % 200 == 0 or i == total:
                    print({"indicator": indicator, "progress": f"{i}/{total}", "ok_chunks": len(frames)}, flush=True)

    if frames:
        all_df = pd.concat(frames, ignore_index=True)
        all_df["ts_code"] = all_df["ts_code"].astype(str).str.zfill(6)
        all_df["trade_date"] = pd.to_datetime(all_df["trade_date"])
        all_df = (
            all_df.sort_values(["ts_code", "trade_date"])
            .drop_duplicates(["ts_code", "trade_date"], keep="last")
            .reset_index(drop=True)
        )
        all_df = all_df[all_df["ts_code"].isin(symbols)].copy()
    else:
        all_df = pd.DataFrame(columns=["trade_date", "value", "ts_code"])

    all_df.to_parquet(out_path, index=False)
    return all_df


def asof_merge_daily(base: pd.DataFrame, val: pd.DataFrame, col_name: str, n_jobs: int = -1) -> pd.DataFrame:
    if val.empty:
        out = base.copy()
        out[col_name] = np.nan
        return out

    left = base.sort_values(["ts_code", "trade_date"]).copy()
    right = val.sort_values(["ts_code", "trade_date"]).copy()

    def _merge_one(code: str, g: pd.DataFrame) -> pd.DataFrame:
        rv = right[right["ts_code"] == code][["trade_date", "value"]]
        if rv.empty:
            gg = g.copy()
            gg[col_name] = np.nan
            return gg
        m = pd.merge_asof(
            g.sort_values("trade_date"),
            rv.sort_values("trade_date"),
            on="trade_date",
            direction="backward",
            allow_exact_matches=True,
        )
        m[col_name] = m["value"]
        return m.drop(columns=["value"])

    groups = [(code, g.copy()) for code, g in left.groupby("ts_code", group_keys=False)]
    outs = Parallel(n_jobs=n_jobs, backend="loky", verbose=0)(
        delayed(_merge_one)(code, g) for code, g in groups
    )
    return pd.concat(outs, ignore_index=True)


def build_features(raw: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    x = raw.sort_values(["ts_code", "trade_date"]).copy()
    g = x.groupby("ts_code", group_keys=False)

    x["ret_20"] = g["close"].pct_change(20)
    x["ret_60"] = g["close"].pct_change(60)
    x["vol_20"] = g["close"].transform(lambda s: s.pct_change().rolling(20).std())
    x["amt_20"] = g["amount"].transform(lambda s: s.rolling(20).mean())
    x["trend_gap"] = g["close"].transform(lambda s: s.rolling(20).mean()) / g["close"].transform(lambda s: s.rolling(60).mean()) - 1
    x["fwd_ret_20"] = g["close"].shift(-20) / x["close"] - 1

    feat_raw = ["ret_20", "ret_60", "vol_20", "amt_20", "trend_gap", "pe_ttm", "pb", "mv_total"]
    for c in feat_raw:
        x[f"cs_{c}"] = cs_rank(x, c)

    feats = [f"cs_{c}" for c in feat_raw]
    x = x.dropna(subset=feats + ["fwd_ret_20", "ma20", "ma60"]).copy()
    return x, feats


def walk_forward_score(df: pd.DataFrame, feats: list[str], train_days: int = 504, retrain_gap: int = 20) -> pd.DataFrame:
    dates = sorted(df["trade_date"].unique().tolist())
    out = []
    model = None
    last_train_idx = -10**9

    for i, d in enumerate(dates):
        if i < train_days:
            continue

        if (i - last_train_idx) >= retrain_gap or model is None:
            tr = df[(df["trade_date"] >= dates[i - train_days]) & (df["trade_date"] <= dates[i - 1])]
            if len(tr) < 2000:
                continue
            model = RandomForestRegressor(
                n_estimators=300,
                max_depth=10,
                min_samples_leaf=50,
                random_state=42,
                n_jobs=-1,
            )
            model.fit(tr[feats], tr["fwd_ret_20"])
            last_train_idx = i

        te = df[df["trade_date"] == d].copy()
        if te.empty or model is None:
            continue
        te["ml_score"] = model.predict(te[feats])
        out.append(te[["trade_date", "ts_code", "close", "amount", "ma20", "ma60", "ml_score"]])

    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def backtest_long_hold(scored: pd.DataFrame, top_n: int = 12, hold_days: int = 40, risk_on_th: float = 0.55) -> pd.DataFrame:
    x = scored.sort_values(["ts_code", "trade_date"]).copy()
    x["fwd_ret_1"] = x.groupby("ts_code")["close"].shift(-1) / x["close"] - 1
    dates = sorted(x["trade_date"].unique().tolist())

    h, eq, rows = {}, 1.0, []
    for d in dates:
        day = x[x["trade_date"] == d].copy()
        day = day[(day["amount"] >= 8e8) & (day["close"] >= 5)]
        breadth = float((day["ma20"] > day["ma60"]).mean()) if len(day) else 0.0
        risk_on = breadth >= risk_on_th
        day = day.sort_values("ml_score", ascending=False)

        rr = []
        if h:
            k = day.set_index("ts_code")
            rr = [float(k.loc[c, "fwd_ret_1"]) for c in h if c in k.index and pd.notna(k.loc[c, "fwd_ret_1"])]
        dr = float(np.mean(rr)) if rr else 0.0

        sell = 0
        for c in list(h.keys()):
            h[c] += 1
            if h[c] >= hold_days:
                del h[c]
                sell += 1

        buy = 0
        if risk_on:
            slots = max(0, top_n - len(h))
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
        net = dr - trade_frac * (2.5 + 5.0) / 10000 - (sell / max(top_n, 1)) * 10 / 10000
        eq *= (1 + net)
        rows.append({"trade_date": d, "daily_ret": net, "equity": eq, "risk_on": int(risk_on), "n_hold": len(h)})

    return pd.DataFrame(rows)


def run() -> None:
    cfg = load_config("configs/research.yaml")
    p = Path(cfg.data.all_buyable_path)
    if not p.exists():
        raise FileNotFoundError(p)

    raw = pd.read_parquet(p)
    raw["trade_date"] = pd.to_datetime(raw["trade_date"])
    raw = apply_universe_filters(raw)
    raw = add_basic_factors(raw)

    symbols = sorted(raw["ts_code"].astype(str).str.zfill(6).unique().tolist())
    cache_dir = Path("data/fundamental_cache")

    pe = build_valuation_cache(symbols, "市盈率(TTM)", cache_dir / "pe_ttm.parquet")
    pb = build_valuation_cache(symbols, "市净率", cache_dir / "pb.parquet")
    mv = build_valuation_cache(symbols, "总市值", cache_dir / "mv_total.parquet")

    x = raw.copy()
    print({"phase": "merge", "target": "pe_ttm"}, flush=True)
    x = asof_merge_daily(x, pe, "pe_ttm", n_jobs=-1)
    print({"phase": "merge", "target": "pb"}, flush=True)
    x = asof_merge_daily(x, pb, "pb", n_jobs=-1)
    print({"phase": "merge", "target": "mv_total"}, flush=True)
    x = asof_merge_daily(x, mv, "mv_total", n_jobs=-1)

    df, feats = build_features(x)
    print({"phase": "train_score", "rows": int(len(df)), "symbols": int(df["ts_code"].nunique())}, flush=True)
    scored = walk_forward_score(df, feats, train_days=504, retrain_gap=20)
    if scored.empty:
        raise RuntimeError("no scored rows in stage16")

    curve = backtest_long_hold(scored, top_n=12, hold_days=40, risk_on_th=0.55)
    m = evaluate_curve(curve)

    latest = scored[scored["trade_date"] == scored["trade_date"].max()].copy()
    latest = latest[(latest["amount"] >= 8e8) & (latest["close"] >= 5)]
    latest = latest.sort_values("ml_score", ascending=False)
    picks = latest[["trade_date", "ts_code", "close", "ml_score"]].head(12).copy()
    if len(picks):
        picks["target_weight"] = 1.0 / len(picks)

    out = Path("reports")
    out.mkdir(parents=True, exist_ok=True)
    summary = {
        "status": "stage16_ml_multifactor_fund_ready" if m["ann_return"] > 0 else "stage16_ml_multifactor_fund_weak",
        "oos_metrics": m,
        "oos_days": int(len(curve)),
        "oos_start": str(curve["trade_date"].min()),
        "oos_end": str(curve["trade_date"].max()),
        "params": {"top_n": 12, "hold_days": 40, "risk_on_th": 0.55, "train_days": 504, "retrain_gap": 20},
        "features": feats,
        "coverage": {"symbols": int(raw["ts_code"].nunique()), "pe_rows": int(len(pe)), "pb_rows": int(len(pb)), "mv_rows": int(len(mv))},
    }

    (out / "stage16_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    curve.to_parquet(out / "stage16_curve.parquet", index=False)
    if len(picks):
        picks.to_csv(out / "stage16_picks.csv", index=False)

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if len(picks):
        print(picks.to_string(index=False))


if __name__ == "__main__":
    run()
