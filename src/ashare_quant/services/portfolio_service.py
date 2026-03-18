from __future__ import annotations

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
REPORTS = ROOT / 'reports'


def load_holdings() -> pd.DataFrame:
    p = REPORTS / 'current_holdings.csv'
    if not p.exists():
        return pd.DataFrame(columns=['ts_code', 'name', 'qty', 'cost', 'buy_date'])
    df = pd.read_csv(p)
    for c in ['ts_code', 'name', 'qty', 'cost', 'buy_date']:
        if c not in df.columns:
            df[c] = None
    df['ts_code'] = df['ts_code'].astype(str).str.zfill(6)
    return df[['ts_code', 'name', 'qty', 'cost', 'buy_date']]


def calc_t1_sellable(holdings: pd.DataFrame, trade_date: str | None) -> pd.DataFrame:
    if holdings.empty:
        return holdings.assign(sellable_qty=[], locked_t1_qty=[])
    out = holdings.copy()
    out['buy_date'] = pd.to_datetime(out['buy_date'], errors='coerce')
    t = pd.to_datetime(trade_date) if trade_date else pd.NaT
    out['qty'] = pd.to_numeric(out['qty'], errors='coerce').fillna(0)

    if pd.isna(t):
        out['sellable_qty'] = out['qty']
        out['locked_t1_qty'] = 0
        return out

    same_day = out['buy_date'].dt.date == t.date()
    out['locked_t1_qty'] = out['qty'].where(same_day, 0)
    out['sellable_qty'] = out['qty'] - out['locked_t1_qty']
    return out


def save_execution_progress(rows: list[dict]) -> None:
    p = REPORTS / 'execution_progress.csv'
    df = pd.DataFrame(rows)
    df.to_csv(p, index=False)
