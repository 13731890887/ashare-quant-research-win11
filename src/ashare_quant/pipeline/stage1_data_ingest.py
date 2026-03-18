from __future__ import annotations

import os
from pathlib import Path
import pandas as pd

from ashare_quant.data.universe import STARTER_SYMBOLS
from ashare_quant.data.akshare_loader import fetch_universe_daily as fetch_ak
from ashare_quant.data.tushare_loader import fetch_universe_daily_tushare as fetch_ts
from ashare_quant.data.processing import normalize_and_clean, vendor_prefer_merge


def _safe_fetch_ak(symbols, start_date, end_date):
    try:
        df, errs = fetch_ak(symbols, start_date, end_date)
        return df, errs
    except Exception as e:
        return pd.DataFrame(), [f"akshare fatal: {type(e).__name__}: {e}"]


def _safe_fetch_ts(symbols, start_date, end_date):
    if not os.getenv("TUSHARE_TOKEN"):
        return pd.DataFrame(), ["tushare skipped: missing TUSHARE_TOKEN"]
    try:
        df, errs = fetch_ts(symbols, start_date, end_date)
        return df, errs
    except Exception as e:
        return pd.DataFrame(), [f"tushare fatal: {type(e).__name__}: {e}"]


def run() -> None:
    start_date = "2019-01-01"
    end_date = "2026-03-14"
    symbols = STARTER_SYMBOLS

    ak_df, ak_errs = _safe_fetch_ak(symbols, start_date, end_date)
    ts_df, ts_errs = _safe_fetch_ts(symbols, start_date, end_date)

    combined = pd.concat([d for d in [ak_df, ts_df] if not d.empty], ignore_index=True) if (not ak_df.empty or not ts_df.empty) else pd.DataFrame()
    if combined.empty:
        raise RuntimeError("No data fetched from all vendors")

    cleaned = normalize_and_clean(combined)
    merged = vendor_prefer_merge(cleaned, vendor_priority=["tushare_daily", "eastmoney_hist", "sina_daily"])

    out_dir = Path("data")
    out_dir.mkdir(parents=True, exist_ok=True)
    cleaned.to_parquet(out_dir / "market_daily_multivendor_cleaned.parquet", index=False)
    merged.to_parquet(out_dir / "market_daily_research_ready.parquet", index=False)

    qc = {
        "symbols_requested": len(symbols),
        "rows_ak": int(len(ak_df)),
        "rows_ts": int(len(ts_df)),
        "rows_cleaned": int(len(cleaned)),
        "rows_research_ready": int(len(merged)),
        "symbols_ready": int(merged["ts_code"].nunique()),
        "date_min": str(merged["trade_date"].min()),
        "date_max": str(merged["trade_date"].max()),
        "ak_errors": len(ak_errs),
        "ts_errors": len(ts_errs),
        "vendors": sorted(cleaned["data_vendor"].dropna().unique().tolist()),
    }

    (out_dir / "market_data_qc_summary.json").write_text(__import__("json").dumps(qc, ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "market_data_fetch_errors.log").write_text("\n".join(ak_errs + ts_errs), encoding="utf-8")

    print(qc)


if __name__ == "__main__":
    run()
