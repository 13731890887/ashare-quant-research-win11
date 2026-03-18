from __future__ import annotations

from pathlib import Path
import json
import numpy as np
import pandas as pd


def run() -> None:
    p = Path("data/market_daily_multivendor_cleaned.parquet")
    if not p.exists():
        raise FileNotFoundError(p)
    df = pd.read_parquet(p)

    # Keep rows where at least two vendors exist for same symbol/date
    c = df.groupby(["trade_date", "ts_code"]) ["data_vendor"].nunique().reset_index(name="n_vendor")
    k = c[c["n_vendor"] >= 2][["trade_date", "ts_code"]]
    x = df.merge(k, on=["trade_date", "ts_code"], how="inner")

    if x.empty:
        out = {"message": "no overlap rows across vendors"}
    else:
        pivot = x.pivot_table(index=["trade_date", "ts_code"], columns="data_vendor", values="close", aggfunc="first")
        cols = [c for c in pivot.columns if c is not None]
        base = "tushare_daily" if "tushare_daily" in cols else cols[0]
        rows = []
        for c in cols:
            if c == base:
                continue
            z = pivot[[base, c]].dropna()
            if z.empty:
                continue
            abs_diff = (z[base] - z[c]).abs()
            rel_bp = (abs_diff / z[base].replace(0, np.nan)).dropna() * 10000
            rows.append({
                "base_vendor": base,
                "cmp_vendor": c,
                "overlap_rows": int(len(z)),
                "median_abs_diff": float(abs_diff.median()),
                "p95_abs_diff": float(abs_diff.quantile(0.95)),
                "median_rel_bp": float(rel_bp.median()) if len(rel_bp) else None,
                "p95_rel_bp": float(rel_bp.quantile(0.95)) if len(rel_bp) else None,
            })
        out = {
            "overlap_symbol_dates": int(len(pivot)),
            "vendors": cols,
            "comparisons": rows,
        }

    out_dir = Path("reports")
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "data_consistency_report_v1.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    run()
