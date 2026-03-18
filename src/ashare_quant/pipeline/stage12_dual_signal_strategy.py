from __future__ import annotations

from pathlib import Path
import json

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor

from ashare_quant.backtest.simulator_realistic import realistic_topn_backtest
from ashare_quant.config.research_config import load_config
from ashare_quant.factors.price_volume import add_basic_factors
from ashare_quant.reporting.metrics import evaluate_curve
from ashare_quant.universe.filtering import apply_universe_filters
from ashare_quant.utils.ranking import cs_rank

try:
    from xgboost import XGBRegressor
except Exception:
    XGBRegressor = None


TOP_N = 10
HOLD_DAYS = 10
PREDICTION_JOURNAL = "stage12_prediction_journal.parquet"
PREDICTION_EVAL_SUMMARY = "stage12_prediction_eval_summary.json"


def _mean_fill(frame: pd.DataFrame, cols: list[str | pd.Series], default: float = 0.5) -> pd.Series:
    parts = []
    for col in cols:
        if isinstance(col, str):
            parts.append(frame[col])
        else:
            parts.append(pd.Series(col, index=frame.index))
    out = pd.concat(parts, axis=1).mean(axis=1)
    return out.fillna(default)


def _load_cached_factor(base: pd.DataFrame, path: Path, out_col: str) -> pd.DataFrame:
    out = base.sort_values(["ts_code", "trade_date"]).copy()
    if not path.exists():
        out[out_col] = np.nan
        return out

    cache = pd.read_parquet(path)
    if cache.empty:
        out[out_col] = np.nan
        return out

    cache = cache.rename(columns={"value": out_col}).copy()
    cache["trade_date"] = pd.to_datetime(cache["trade_date"])
    cache["ts_code"] = cache["ts_code"].astype(str).str.zfill(6)

    cache = cache[["ts_code", "trade_date", out_col]].sort_values(["trade_date", "ts_code"]).reset_index(drop=True)
    out = out.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)
    merged = pd.merge_asof(
        out,
        cache,
        on="trade_date",
        by="ts_code",
        direction="backward",
        allow_exact_matches=True,
    )
    return merged


def enrich_with_fundamentals(raw: pd.DataFrame) -> pd.DataFrame:
    cache_dir = Path("data/fundamental_cache")
    x = _load_cached_factor(raw, cache_dir / "pe_ttm.parquet", "pe_ttm")
    x = _load_cached_factor(x, cache_dir / "pb.parquet", "pb")
    x = _load_cached_factor(x, cache_dir / "mv_total.parquet", "mv_total")
    return x


def build_multifactor_frame(raw: pd.DataFrame) -> pd.DataFrame:
    x = raw.sort_values(["ts_code", "trade_date"]).copy()
    g = x.groupby("ts_code", group_keys=False)

    x["ret_60"] = g["close"].pct_change(60)
    x["ret_120"] = g["close"].pct_change(120)
    x["ma120"] = g["close"].transform(lambda s: s.rolling(120).mean())
    x["vol_20"] = g["close"].transform(lambda s: s.pct_change().rolling(20).std())
    x["vol_60"] = g["close"].transform(lambda s: s.pct_change().rolling(60).std())
    x["amount_20"] = g["amount"].transform(lambda s: s.rolling(20).mean())
    x["amount_60"] = g["amount"].transform(lambda s: s.rolling(60).mean())
    x["turnover_accel"] = x["amount"] / x["amount_20"]
    x["trend_gap_20_60"] = x["ma20"] / x["ma60"] - 1
    x["trend_gap_60_120"] = x["ma60"] / x["ma120"] - 1
    x["distance_ma20"] = x["close"] / x["ma20"] - 1
    x["distance_ma60"] = x["close"] / x["ma60"] - 1
    x["breakout_60"] = x["close"] / g["high"].transform(lambda s: s.rolling(60).max()) - 1
    x["drawdown_20"] = x["close"] / g["close"].transform(lambda s: s.rolling(20).max()) - 1
    x["drawdown_60"] = x["close"] / g["close"].transform(lambda s: s.rolling(60).max()) - 1
    x["fwd_ret_5"] = g["close"].shift(-5) / x["close"] - 1
    x["fwd_ret_10"] = g["close"].shift(-10) / x["close"] - 1

    rank_cols = [
        "ret_5",
        "ret_20",
        "ret_60",
        "ret_120",
        "vol_ratio_20",
        "amount",
        "amount_20",
        "amount_60",
        "turnover_accel",
        "trend_gap_20_60",
        "trend_gap_60_120",
        "distance_ma20",
        "distance_ma60",
        "breakout_60",
        "drawdown_20",
        "drawdown_60",
        "vol_20",
        "vol_60",
        "pe_ttm",
        "pb",
        "mv_total",
    ]
    for col in rank_cols:
        x[f"cs_{col}"] = cs_rank(x, col)

    x["momentum_score"] = _mean_fill(
        x,
        [
            "cs_ret_20",
            "cs_ret_60",
            "cs_ret_120",
            "cs_trend_gap_20_60",
            "cs_trend_gap_60_120",
            "cs_breakout_60",
            "cs_distance_ma20",
        ],
    )
    x["liquidity_score"] = _mean_fill(x, ["cs_amount", "cs_amount_20", "cs_turnover_accel", "cs_vol_ratio_20"])
    x["quality_score"] = _mean_fill(
        x,
        [
            1 - x["cs_vol_20"],
            1 - x["cs_vol_60"],
            x["cs_drawdown_20"],
            x["cs_drawdown_60"],
        ],
    )
    x["value_score"] = _mean_fill(x, [1 - x["cs_pe_ttm"], 1 - x["cs_pb"], 1 - x["cs_mv_total"]])
    x["rule_score"] = (
        0.42 * x["momentum_score"]
        + 0.23 * x["quality_score"]
        + 0.20 * x["liquidity_score"]
        + 0.15 * x["value_score"]
    )
    x["risk_score"] = _mean_fill(
        x,
        [
            x["cs_vol_20"],
            x["cs_vol_60"],
            1 - x["cs_drawdown_20"],
            1 - x["cs_trend_gap_20_60"],
        ],
    )
    x["buyable_today"] = (
        (x["close"] >= 5)
        & (x["amount"] >= 8e8)
        & (x["ma20"] > x["ma60"])
        & (x["ret_20"] > 0)
        & (x["drawdown_20"] > -0.18)
    )
    x["conviction_score"] = (x["rule_score"] * 100).round(2)

    need = [
        "ret_20",
        "ret_60",
        "ret_120",
        "ma20",
        "ma60",
        "ma120",
        "amount_20",
        "rule_score",
        "risk_score",
    ]
    x = x.dropna(subset=need).copy()
    return x


def fit_predict_multifactor(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    feature_cols = [
        "momentum_score",
        "quality_score",
        "liquidity_score",
        "value_score",
        "rule_score",
        "risk_score",
        "ret_20",
        "ret_60",
        "ret_120",
        "amount_20",
        "trend_gap_20_60",
        "trend_gap_60_120",
        "distance_ma20",
        "distance_ma60",
        "drawdown_20",
        "drawdown_60",
        "vol_20",
        "vol_60",
        "pe_ttm",
        "pb",
        "mv_total",
    ]
    train = df[(df["trade_date"] >= "2022-01-01") & (df["trade_date"] <= "2024-12-31")].copy()
    pred = df[df["trade_date"] >= "2025-01-01"].copy()
    if train.empty or pred.empty:
        raise RuntimeError("insufficient train/pred window for stage12 multifactor model")

    train = train.dropna(subset=feature_cols + ["fwd_ret_10"]).copy()
    pred = pred.dropna(subset=feature_cols).copy()
    if train.empty or pred.empty:
        raise RuntimeError("feature frame became empty after NA filtering")

    model_info = {"model": "random_forest_cpu"}
    if XGBRegressor is not None:
        model = XGBRegressor(
            n_estimators=500,
            max_depth=8,
            learning_rate=0.05,
            subsample=0.85,
            colsample_bytree=0.85,
            reg_alpha=0.05,
            reg_lambda=1.2,
            min_child_weight=4,
            objective="reg:squarederror",
            tree_method="hist",
            device="cuda",
            random_state=42,
        )
        model_info = {"model": "xgboost_cuda"}
    else:
        model = RandomForestRegressor(
            n_estimators=240,
            max_depth=10,
            min_samples_leaf=60,
            random_state=42,
            n_jobs=-1,
        )

    model.fit(train[feature_cols], train["fwd_ret_10"])
    pred["ml_score"] = model.predict(pred[feature_cols])
    pred["final_score"] = 0.65 * pred["ml_score"] + 0.35 * pred["rule_score"]
    pred["stock_rank"] = pred.groupby("trade_date")["final_score"].rank(method="first", ascending=False)
    return pred, {**model_info, "train_rows": int(len(train)), "pred_rows": int(len(pred))}


def load_calibration(out_dir: Path) -> dict:
    journal_path = out_dir / PREDICTION_JOURNAL
    if not journal_path.exists():
        return {"bias": 0.0, "slope": 1.0, "sample_size": 0, "mae": None, "corr": None}

    journal = pd.read_parquet(journal_path)
    matured = journal.dropna(subset=["predicted_ret_10", "actual_ret_10"]).copy()
    if len(matured) < 30:
        return {"bias": 0.0, "slope": 1.0, "sample_size": int(len(matured)), "mae": None, "corr": None}

    x = matured["predicted_ret_10"].astype(float).to_numpy()
    y = matured["actual_ret_10"].astype(float).to_numpy()
    slope, bias = np.polyfit(x, y, deg=1)
    pred_adj = bias + slope * x
    mae = float(np.mean(np.abs(pred_adj - y)))
    corr = float(np.corrcoef(x, y)[0, 1]) if len(matured) > 1 else None
    return {
        "bias": float(bias),
        "slope": float(slope),
        "sample_size": int(len(matured)),
        "mae": mae,
        "corr": corr,
    }


def apply_calibration(scored: pd.DataFrame, calibration: dict) -> pd.DataFrame:
    out = scored.copy()
    bias = float(calibration.get("bias", 0.0))
    slope = float(calibration.get("slope", 1.0))
    out["expected_ret_10"] = bias + slope * out["ml_score"]
    out["expected_ret_10"] = out["expected_ret_10"].clip(lower=-0.30, upper=0.50)
    out["final_score"] = 0.65 * out["expected_ret_10"] + 0.35 * out["rule_score"]
    out["stock_rank"] = out.groupby("trade_date")["final_score"].rank(method="first", ascending=False)
    return out


def update_prediction_journal(
    out_dir: Path,
    latest_picks: pd.DataFrame,
    scored: pd.DataFrame,
    calibration: dict,
) -> tuple[pd.DataFrame, dict]:
    journal_path = out_dir / PREDICTION_JOURNAL
    if journal_path.exists():
        journal = pd.read_parquet(journal_path)
    else:
        journal = pd.DataFrame()

    if not journal.empty:
        journal["trade_date"] = pd.to_datetime(journal["trade_date"])
        if "maturity_date" in journal.columns:
            journal["maturity_date"] = pd.to_datetime(journal["maturity_date"])

    latest_records = latest_picks.copy()
    latest_records["trade_date"] = pd.to_datetime(latest_records["trade_date"])
    latest_records["maturity_date"] = latest_records["trade_date"] + pd.offsets.BDay(HOLD_DAYS)
    latest_records["predicted_ret_10"] = latest_records["ml_score"].astype(float)
    latest_records["expected_ret_10"] = latest_records["expected_ret_10"].astype(float)
    latest_records["actual_ret_10"] = np.nan
    latest_records["prediction_error"] = np.nan
    latest_records["abs_error"] = np.nan
    latest_records["is_correct_direction"] = np.nan
    latest_records["calibration_bias"] = float(calibration.get("bias", 0.0))
    latest_records["calibration_slope"] = float(calibration.get("slope", 1.0))
    latest_records["model_name"] = calibration.get("model_name", "multifactor_top10")

    keep_cols = [
        "trade_date",
        "maturity_date",
        "ts_code",
        "close",
        "latest_rank",
        "momentum_score",
        "quality_score",
        "liquidity_score",
        "value_score",
        "rule_score",
        "risk_score",
        "ml_score",
        "predicted_ret_10",
        "expected_ret_10",
        "actual_ret_10",
        "prediction_error",
        "abs_error",
        "is_correct_direction",
        "calibration_bias",
        "calibration_slope",
        "model_name",
    ]
    latest_records = latest_records[keep_cols].copy()

    journal = pd.concat([journal, latest_records], ignore_index=True) if not journal.empty else latest_records
    journal = journal.sort_values(["trade_date", "ts_code"]).drop_duplicates(["trade_date", "ts_code"], keep="last").reset_index(drop=True)

    realized = scored[["trade_date", "ts_code", "fwd_ret_10"]].copy()
    realized = realized.rename(columns={"trade_date": "realized_from_date", "fwd_ret_10": "actual_ret_10_new"})
    realized["realized_from_date"] = pd.to_datetime(realized["realized_from_date"])
    journal = journal.merge(
        realized,
        left_on=["trade_date", "ts_code"],
        right_on=["realized_from_date", "ts_code"],
        how="left",
    )
    journal["actual_ret_10"] = journal["actual_ret_10"].fillna(journal["actual_ret_10_new"])
    journal = journal.drop(columns=["realized_from_date", "actual_ret_10_new"])

    matured_mask = journal["actual_ret_10"].notna()
    journal.loc[matured_mask, "prediction_error"] = (
        journal.loc[matured_mask, "expected_ret_10"] - journal.loc[matured_mask, "actual_ret_10"]
    )
    journal.loc[matured_mask, "abs_error"] = journal.loc[matured_mask, "prediction_error"].abs()
    journal.loc[matured_mask, "is_correct_direction"] = (
        (journal.loc[matured_mask, "expected_ret_10"] > 0) == (journal.loc[matured_mask, "actual_ret_10"] > 0)
    )
    journal.to_parquet(journal_path, index=False)

    matured = journal[matured_mask].copy()
    if matured.empty:
        eval_summary = {
            "matured_predictions": 0,
            "mae": None,
            "rmse": None,
            "bias": None,
            "directional_accuracy": None,
            "corr": None,
        }
    else:
        err = matured["prediction_error"].astype(float)
        pred = matured["expected_ret_10"].astype(float)
        act = matured["actual_ret_10"].astype(float)
        eval_summary = {
            "matured_predictions": int(len(matured)),
            "mae": float(err.abs().mean()),
            "rmse": float(np.sqrt(np.mean(np.square(err)))),
            "bias": float(err.mean()),
            "directional_accuracy": float(matured["is_correct_direction"].astype(float).mean()),
            "corr": float(np.corrcoef(pred, act)[0, 1]) if len(matured) > 1 else None,
        }
    (out_dir / PREDICTION_EVAL_SUMMARY).write_text(json.dumps(eval_summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return journal, eval_summary


def build_latest_ranked(scored: pd.DataFrame) -> tuple[pd.Timestamp, pd.DataFrame]:
    latest_date = scored["trade_date"].max()
    latest = scored[scored["trade_date"] == latest_date].copy()
    latest = latest.sort_values(["buyable_today", "final_score"], ascending=[False, False]).reset_index(drop=True)
    latest["latest_rank"] = np.arange(1, len(latest) + 1)
    latest["signal_label"] = np.where(
        latest["buyable_today"] & (latest["final_score"] >= latest["final_score"].quantile(0.8)),
        "BUY",
        np.where(latest["final_score"] >= latest["final_score"].quantile(0.5), "WATCH", "PASS"),
    )
    return latest_date, latest


def generate_actions(latest_ranked: pd.DataFrame, current_holdings: set[str], top_n: int = TOP_N) -> dict:
    picks = latest_ranked[latest_ranked["buyable_today"]].head(top_n).copy()
    buy = [c for c in picks["ts_code"].astype(str).tolist() if c not in current_holdings]

    hold, sell = [], []
    table = latest_ranked.set_index("ts_code")
    threshold = float(picks["final_score"].min()) if not picks.empty else 0.0
    for code in sorted(current_holdings):
        if code not in table.index:
            sell.append({"ts_code": code, "reason": "missing_today"})
            continue
        row = table.loc[code]
        if bool(row["buyable_today"]) and float(row["final_score"]) >= threshold:
            hold.append(
                {
                    "ts_code": code,
                    "final_score": round(float(row["final_score"]), 4),
                    "rule_score": round(float(row["rule_score"]), 4),
                    "risk_score": round(float(row["risk_score"]), 4),
                }
            )
        else:
            sell.append(
                {
                    "ts_code": code,
                    "reason": (
                        f"out_of_top10(score={float(row['final_score']):.4f},"
                        f"risk={float(row['risk_score']):.4f},buyable={bool(row['buyable_today'])})"
                    ),
                }
            )
    return {"buy": buy, "hold": hold, "sell": sell}


def summarize_stock(latest_ranked: pd.DataFrame, symbol: str) -> dict | None:
    row = latest_ranked[latest_ranked["ts_code"].astype(str).str.zfill(6) == symbol]
    if row.empty:
        return None

    r = row.iloc[0]
    decision = "观望"
    if bool(r["buyable_today"]) and int(r["latest_rank"]) <= TOP_N:
        decision = "可买入"
    elif bool(r["buyable_today"]) and float(r["rule_score"]) >= 0.6:
        decision = "候选观察"

    return {
        "trade_date": str(pd.to_datetime(r["trade_date"]).date()),
        "ts_code": symbol,
        "latest_rank": int(r["latest_rank"]),
        "decision": decision,
        "buyable_today": bool(r["buyable_today"]),
        "close": round(float(r["close"]), 3),
        "ret_20": round(float(r["ret_20"]), 4),
        "ret_60": round(float(r["ret_60"]), 4),
        "momentum_score": round(float(r["momentum_score"]), 4),
        "quality_score": round(float(r["quality_score"]), 4),
        "liquidity_score": round(float(r["liquidity_score"]), 4),
        "value_score": round(float(r["value_score"]), 4),
        "ml_score": round(float(r["ml_score"]), 6),
        "final_score": round(float(r["final_score"]), 6),
        "rule_score": round(float(r["rule_score"]), 4),
        "risk_score": round(float(r["risk_score"]), 4),
        "trend_ok": bool(r["ma20"] > r["ma60"]),
        "notes": [
            "多因子评分综合考虑趋势、动量、流动性、估值和波动。",
            "buyable_today 为真且排名进入前10时，才视为当天可执行候选。",
        ],
    }


def run() -> None:
    cfg = load_config("configs/research.yaml")
    data_path = Path(cfg.data.all_buyable_path)
    if not data_path.exists():
        raise FileNotFoundError(data_path)

    raw = pd.read_parquet(data_path)
    raw["trade_date"] = pd.to_datetime(raw["trade_date"])
    raw["ts_code"] = raw["ts_code"].astype(str).str.zfill(6)

    raw = apply_universe_filters(raw)
    raw = add_basic_factors(raw)
    raw = enrich_with_fundamentals(raw)

    out = Path("reports")
    out.mkdir(parents=True, exist_ok=True)

    feature_frame = build_multifactor_frame(raw)
    scored, model_meta = fit_predict_multifactor(feature_frame)
    calibration = load_calibration(out)
    calibration["model_name"] = str(model_meta.get("model", "multifactor_top10"))
    scored = apply_calibration(scored, calibration)
    latest_date, latest_ranked = build_latest_ranked(scored)
    latest_picks = latest_ranked[latest_ranked["buyable_today"]].head(TOP_N).copy()
    latest_top50 = latest_ranked.head(50).copy()

    holdings_file = Path("reports/current_holdings.csv")
    if holdings_file.exists():
        holdings = pd.read_csv(holdings_file)
        current_holdings = set(holdings["ts_code"].astype(str).str.zfill(6).tolist())
    else:
        current_holdings = set()

    actions = generate_actions(latest_ranked, current_holdings, top_n=TOP_N)

    bt_universe = scored[scored["buyable_today"]].copy()
    bt_universe["rule_score"] = bt_universe["final_score"]
    curve = realistic_topn_backtest(bt_universe, top_n=TOP_N, hold_days=HOLD_DAYS)
    metrics = evaluate_curve(curve)

    latest_top50[
        [
            "trade_date",
            "ts_code",
            "close",
            "ret_20",
            "ret_60",
            "momentum_score",
            "quality_score",
            "liquidity_score",
            "value_score",
            "ml_score",
            "expected_ret_10",
            "final_score",
            "rule_score",
            "risk_score",
            "buyable_today",
            "signal_label",
            "latest_rank",
        ]
    ].to_csv(out / "stage12_dual_top50.csv", index=False)

    latest_picks[
        [
            "trade_date",
            "ts_code",
            "close",
            "ret_20",
            "ret_60",
            "momentum_score",
            "quality_score",
            "liquidity_score",
            "value_score",
            "ml_score",
            "expected_ret_10",
            "final_score",
            "rule_score",
            "risk_score",
            "latest_rank",
        ]
    ].to_csv(out / "stage12_top10.csv", index=False)

    latest_ranked[
        [
            "trade_date",
            "ts_code",
            "close",
            "amount",
            "ret_20",
            "ret_60",
            "ma20",
            "ma60",
            "ma120",
            "momentum_score",
            "quality_score",
            "liquidity_score",
            "value_score",
            "ml_score",
            "expected_ret_10",
            "final_score",
            "rule_score",
            "risk_score",
            "conviction_score",
            "buyable_today",
            "signal_label",
            "latest_rank",
        ]
    ].to_csv(out / "stage12_latest_universe.csv", index=False)

    curve.to_parquet(out / "stage12_backtest_curve.parquet", index=False)
    backtest_summary = {
        "strategy": "multifactor_top10",
        "top_n": TOP_N,
        "hold_days": HOLD_DAYS,
        "model_meta": model_meta,
        **metrics,
        "start_date": str(pd.to_datetime(curve["trade_date"].min()).date()) if not curve.empty else None,
        "end_date": str(pd.to_datetime(curve["trade_date"].max()).date()) if not curve.empty else None,
    }
    (out / "stage12_backtest_summary.json").write_text(
        json.dumps(backtest_summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    stock_focus = {
        code: summarize_stock(latest_ranked, code)
        for code in latest_picks["ts_code"].astype(str).tolist()
    }
    (out / "stage12_stock_analysis.json").write_text(
        json.dumps(stock_focus, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _, eval_summary = update_prediction_journal(out, latest_picks, scored, calibration)

    summary = {
        "trade_date": str(pd.to_datetime(latest_date).date()),
        "strategy": "multifactor_top10",
        "n_candidates_top50": int(len(latest_top50)),
        "buy_count": len(actions["buy"]),
        "hold_count": len(actions["hold"]),
        "sell_count": len(actions["sell"]),
        "top10_count": int(len(latest_picks)),
        "top10_min_score": round(float(latest_picks["final_score"].min()), 4) if not latest_picks.empty else None,
        "top10_codes": latest_picks["ts_code"].astype(str).tolist(),
        "calibration": calibration,
        "prediction_eval": eval_summary,
        "backtest": backtest_summary,
    }
    (out / "stage12_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "stage12_actions.json").write_text(
        json.dumps({"trade_date": str(pd.to_datetime(latest_date).date()), **actions}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if not latest_picks.empty:
        print(latest_picks[["ts_code", "final_score", "rule_score", "risk_score", "latest_rank"]].to_string(index=False))


if __name__ == "__main__":
    run()
