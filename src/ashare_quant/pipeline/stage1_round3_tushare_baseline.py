from __future__ import annotations

from ashare_quant.data.tushare_loader import fetch_universe_daily_tushare
from ashare_quant.universe.filtering import apply_universe_filters
from ashare_quant.factors.price_volume import add_basic_factors
from ashare_quant.scoring.rules import score_transparent_rules
from ashare_quant.backtest.simulator_realistic import realistic_topn_backtest
from ashare_quant.reporting.metrics import evaluate_curve
from ashare_quant.experiments.tracker import log_experiment


def run() -> None:
    symbols = [
        "000001", "000002", "000333", "000651", "000858", "002594", "300750", "300059",
        "600000", "600036", "600519", "600900", "601318", "601398", "601888", "603259",
    ]
    start_date = "2019-01-01"
    end_date = "2026-03-14"

    raw, fetch_errors = fetch_universe_daily_tushare(symbols, start_date, end_date)
    df = apply_universe_filters(raw)
    df = add_basic_factors(df)
    df = score_transparent_rules(df)
    curve = realistic_topn_backtest(df, top_n=8, hold_days=5)
    metrics = evaluate_curve(curve)
    metrics["data_source"] = "tushare_live"
    metrics["fetch_error_count"] = len(fetch_errors)
    log_experiment(
        "stage1_round3_tushare_baseline",
        {"symbols": len(symbols), "top_n": 8, "hold_days": 5, "start": start_date, "end": end_date},
        metrics,
    )
    print(metrics)
    if fetch_errors:
        print("sample_fetch_errors:", fetch_errors[:3])


if __name__ == "__main__":
    run()
