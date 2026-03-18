from __future__ import annotations

from pathlib import Path
import json
import math
import pandas as pd

from ashare_quant.data.tushare_loader import _init_pro
from ashare_quant.data.processing import normalize_and_clean


def get_buyable_symbols() -> pd.DataFrame:
    pro = _init_pro()
    sb = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,market,list_date')
    sb = sb.copy()
    sb = sb[~sb['name'].astype(str).str.contains('ST|\*ST|退', na=False)]
    sb = sb[sb['market'].isin(['主板', '创业板', '科创板'])]
    sb = sb.drop_duplicates('symbol').reset_index(drop=True)
    return sb


def fetch_daily_batch(pro, ts_codes: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    frames = []
    for code in ts_codes:
        try:
            d = pro.daily(ts_code=code, start_date=start_date, end_date=end_date)
            if d is None or d.empty:
                continue
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
            frames.append(x)
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def run() -> None:
    out = Path('data/stage4_all_buyable')
    out.mkdir(parents=True, exist_ok=True)

    sb = get_buyable_symbols()
    sb.to_csv(out / 'buyable_symbols.csv', index=False)
    ts_codes = sb['ts_code'].tolist()

    pro = _init_pro()
    start_date, end_date = '20210101', '20260314'
    batch_size = 120
    n = len(ts_codes)
    n_batches = math.ceil(n / batch_size)

    existing = {int(x.stem.split('_')[-1]) for x in out.glob('daily_chunk_*.parquet')}
    rows_total = 0
    for i in range(n_batches):
        if i in existing:
            continue
        chunk = ts_codes[i*batch_size:(i+1)*batch_size]
        df = fetch_daily_batch(pro, chunk, start_date, end_date)
        if df.empty:
            continue
        df = normalize_and_clean(df)
        rows_total += len(df)
        df.to_parquet(out / f'daily_chunk_{i:04d}.parquet', index=False)
        print({'batch': i, 'rows_total': rows_total}, flush=True)

    parts = sorted(out.glob('daily_chunk_*.parquet'))
    if not parts:
        raise RuntimeError('no data chunks fetched')
    all_df = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
    all_df = all_df.sort_values(['trade_date', 'ts_code']).reset_index(drop=True)
    all_df.to_parquet(out / 'market_daily_all_buyable_20210101_20260314.parquet', index=False)

    meta = {
        'symbols_buyable': int(len(sb)),
        'rows_total': int(len(all_df)),
        'symbols_ready': int(all_df['ts_code'].nunique()),
        'date_min': str(all_df['trade_date'].min()),
        'date_max': str(all_df['trade_date'].max()),
        'path': str(out / 'market_daily_all_buyable_20210101_20260314.parquet')
    }
    (out / 'meta.json').write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')
    print(meta)


if __name__ == '__main__':
    run()
