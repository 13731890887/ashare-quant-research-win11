from __future__ import annotations

from pathlib import Path
import json
import pandas as pd
import akshare as ak

from ashare_quant.data.akshare_loader import fetch_universe_daily
from ashare_quant.data.universe import STARTER_SYMBOLS
from ashare_quant.data.processing import normalize_and_clean


def build_liquid_universe(max_symbols: int = 120) -> list[str]:
    try:
        spot = ak.stock_zh_a_spot_em()
        s = spot.copy().rename(columns={"代码": "symbol", "名称": "name", "成交额": "amount"})
        s = s[~s["name"].astype(str).str.contains("ST|退", na=False)]
        s = s[~s["symbol"].astype(str).str.startswith("8")]
        s["amount"] = pd.to_numeric(s["amount"], errors="coerce")
        s = s.dropna(subset=["amount"]).sort_values("amount", ascending=False)
        symbols = s["symbol"].astype(str).head(max_symbols).tolist()
        if symbols:
            return symbols
    except Exception:
        pass
    return STARTER_SYMBOLS


def enrich_status_fields(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    x["is_limit_up_close"] = (x["close"] >= x["up_limit"] * 0.999).fillna(False)
    x["is_limit_down_close"] = (x["close"] <= x["down_limit"] * 1.001).fillna(False)
    return x


def run() -> None:
    symbols = build_liquid_universe(120)
    start_date, end_date = "2019-01-01", "2026-03-14"

    raw, errs = fetch_universe_daily(symbols, start_date, end_date)
    clean = normalize_and_clean(raw)
    clean = enrich_status_fields(clean)

    out = Path("data")
    out.mkdir(parents=True, exist_ok=True)
    clean.to_parquet(out / "market_daily_stage3_akshare120.parquet", index=False)

    meta = {
        "symbols_requested": len(symbols),
        "symbols_ready": int(clean["ts_code"].nunique()),
        "rows": int(len(clean)),
        "date_min": str(clean["trade_date"].min()),
        "date_max": str(clean["trade_date"].max()),
        "vendor_counts": {k: int(v) for k, v in clean["data_vendor"].value_counts().to_dict().items()},
        "errors": len(errs),
    }
    (out / "stage3_akshare120_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "stage3_akshare120_symbols.txt").write_text("\n".join(symbols), encoding="utf-8")
    print(meta)


if __name__ == "__main__":
    run()
