from __future__ import annotations

import json
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
REPORTS = ROOT / 'reports'
DATA = ROOT / 'data' / 'stage4_all_buyable'


def read_json(path: Path):
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding='utf-8'))


def load_stage12_summary(): return read_json(REPORTS / 'stage12_summary.json')
def load_stage12_actions(): return read_json(REPORTS / 'stage12_actions.json')
def load_refresh_meta(): return read_json(DATA / 'refresh_meta.json')
def load_stage6_summary(): return read_json(REPORTS / 'stage6_summary.json')
def load_stage7_summary(): return read_json(REPORTS / 'stage7_repair_summary.json')
def load_stage8_summary(): return read_json(REPORTS / 'stage8_ml_summary.json')
def load_stage9_summary(): return read_json(REPORTS / 'stage9_wf_ml_summary.json')
def load_stage10_summary(): return read_json(REPORTS / 'stage10_diagnosis_summary.json')
def load_stage11_summary(): return read_json(REPORTS / 'stage11_summary.json')


def load_top50() -> pd.DataFrame:
    p = REPORTS / 'stage12_dual_top50.csv'
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_csv(p)
    if 'ts_code' in df.columns:
        df['ts_code'] = df['ts_code'].astype(str).str.zfill(6)
    return df


def load_curve_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def derive_account_overview() -> dict:
    """Simulation-only overview derived from latest curve (placeholder-safe)."""
    curve_path = REPORTS / 'stage11_curve.parquet'
    if not curve_path.exists():
        return {
            'total_asset': None,
            'available_cash': None,
            'position_mv': None,
            'daily_pnl': None,
            'cum_pnl': None,
            'ann': None,
            'mdd': None,
        }
    c = pd.read_parquet(curve_path)
    if c.empty:
        return {}
    c = c.sort_values('trade_date').reset_index(drop=True)
    eq = c['equity']
    initial = 1.0
    daily = float(eq.iloc[-1] - eq.iloc[-2]) if len(eq) >= 2 else 0.0
    return {
        'total_asset': float(eq.iloc[-1]),
        'available_cash': None,
        'position_mv': None,
        'daily_pnl': daily,
        'cum_pnl': float(eq.iloc[-1] - initial),
        'ann': None,
        'mdd': float((eq / eq.cummax() - 1).min()),
    }
