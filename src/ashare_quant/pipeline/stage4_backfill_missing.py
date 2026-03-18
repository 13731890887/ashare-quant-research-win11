from __future__ import annotations

from pathlib import Path
import json
import time
import pandas as pd

from ashare_quant.data.tushare_loader import _init_pro
from ashare_quant.data.processing import normalize_and_clean


def fetch_one(pro, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    d = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    if d is None or d.empty:
        return pd.DataFrame()
    x = pd.DataFrame()
    x['trade_date'] = pd.to_datetime(d['trade_date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
    x['open'] = d['open']
    x['high'] = d['high']
    x['low'] = d['low']
    x['close'] = d['close']
    x['volume'] = d['vol'] * 100
    x['amount'] = d['amount'] * 1000
    x['ts_code'] = d['ts_code'].str.replace('.SZ', '', regex=False).str.replace('.SH', '', regex=False)
    x['is_st'] = False
    x['is_suspended'] = False
    x['up_limit'] = x['close'] * 1.1
    x['down_limit'] = x['close'] * 0.9
    x['data_vendor'] = 'tushare_daily'
    return normalize_and_clean(x)


def run() -> None:
    base = Path('data/stage4_all_buyable')
    sym = pd.read_csv(base / 'buyable_symbols.csv')
    allp = pd.read_parquet(base / 'market_daily_all_buyable_20210101_20260314.parquet', columns=['ts_code'])

    req = set(sym['symbol'].astype(str).str.zfill(6))
    ready = set(allp['ts_code'].astype(str).str.zfill(6).unique())
    missing = sorted(req - ready)

    pro = _init_pro()
    out_dir = base / 'backfill_parts'
    out_dir.mkdir(parents=True, exist_ok=True)

    ok = 0
    fail = []
    start_date, end_date = '20210101', '20260314'

    for i, code in enumerate(missing, 1):
        ts_code = f'{code}.SH' if code.startswith('6') else f'{code}.SZ'
        got = False
        for _ in range(3):
            try:
                d = fetch_one(pro, ts_code, start_date, end_date)
                if not d.empty:
                    d.to_parquet(out_dir / f'{code}.parquet', index=False)
                    ok += 1
                got = True
                break
            except Exception:
                time.sleep(0.8)
        if not got:
            fail.append(code)
        if i % 100 == 0:
            print({'progress': i, 'total_missing': len(missing), 'ok': ok, 'fail': len(fail)}, flush=True)

    # merge if any new parts
    parts = sorted((base / 'backfill_parts').glob('*.parquet'))
    if parts:
        old = pd.read_parquet(base / 'market_daily_all_buyable_20210101_20260314.parquet')
        new = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
        merged = pd.concat([old, new], ignore_index=True)
        merged = merged.sort_values(['trade_date', 'ts_code']).drop_duplicates(['trade_date', 'ts_code'], keep='first').reset_index(drop=True)
        merged.to_parquet(base / 'market_daily_all_buyable_20210101_20260314.parquet', index=False)

    final = pd.read_parquet(base / 'market_daily_all_buyable_20210101_20260314.parquet', columns=['ts_code'])
    final_ready = set(final['ts_code'].astype(str).str.zfill(6).unique())
    meta = {
        'requested': len(req),
        'ready_after_backfill': len(final_ready),
        'missing_after_backfill': len(req - final_ready),
        'newly_filled': ok,
        'failed_codes': len(fail),
    }
    (base / 'backfill_meta.json').write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')
    (base / 'backfill_failed_codes.txt').write_text('\n'.join(fail), encoding='utf-8')
    print(meta)


if __name__ == '__main__':
    run()
