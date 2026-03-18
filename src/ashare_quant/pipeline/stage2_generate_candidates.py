from __future__ import annotations

from pathlib import Path
import pandas as pd

from ashare_quant.universe.filtering import apply_universe_filters
from ashare_quant.factors.price_volume import add_basic_factors
from ashare_quant.scoring.rules import score_transparent_rules


def run(top_n: int = 8) -> None:
    p = Path("data/market_daily_research_ready.parquet")
    if not p.exists():
        raise FileNotFoundError(f"missing dataset: {p}")

    df = pd.read_parquet(p)
    df = apply_universe_filters(df)
    df = add_basic_factors(df)
    df = score_transparent_rules(df)

    # execution-friendly filters for manual paper trading
    f = df.copy()
    f = f[(f["ma20"] > f["ma60"]) & (f["ret_20"] > 0)]
    f = f[f["vol_ratio_20"].fillna(0) > 0.8]

    latest = f["trade_date"].max()
    day = f[f["trade_date"] == latest].copy()
    day = day.sort_values("rule_score", ascending=False)

    picks = day[["trade_date", "ts_code", "close", "rule_score", "ret_20", "amount", "vol_ratio_20"]].head(top_n).copy()
    if picks.empty:
        raise RuntimeError("No picks generated after filters")

    picks["target_weight"] = 1.0 / len(picks)

    out = Path("reports")
    out.mkdir(parents=True, exist_ok=True)
    out_csv = out / "daily_picks_stage2.csv"
    picks.to_csv(out_csv, index=False)

    summary = {
        "trade_date": str(latest),
        "n_picks": int(len(picks)),
        "avg_score": float(picks["rule_score"].mean()),
        "avg_ret20": float(picks["ret_20"].mean()),
        "output": str(out_csv),
    }
    (out / "daily_picks_stage2_summary.json").write_text(__import__("json").dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(picks.to_string(index=False))
    print(summary)


if __name__ == "__main__":
    run()
