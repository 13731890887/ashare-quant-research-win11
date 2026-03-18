from __future__ import annotations

from pathlib import Path
import json
import os
import pandas as pd

from ashare_quant.data.tushare_loader import _init_pro


def _norm_code_no_suffix(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
        .str.replace('.SZ', '', regex=False)
        .str.replace('.SH', '', regex=False)
        .str.zfill(6)
    )


def run() -> None:
    base = Path('data/stage4_all_buyable')
    data_path = base / 'market_daily_all_buyable_20210101_20260314.parquet'
    sym_path = base / 'buyable_symbols.csv'
    if not data_path.exists() or not sym_path.exists():
        raise FileNotFoundError('missing base dataset or symbol file')

    old = pd.read_parquet(data_path)
    old['trade_date'] = pd.to_datetime(old['trade_date'])
    last_date_dt = old['trade_date'].max()
    start_dt = last_date_dt + pd.Timedelta(days=1)

    end_date_env = os.getenv('STAGE13_END_DATE')
    if end_date_env:
        end_dt = pd.to_datetime(end_date_env)
    else:
        end_dt = pd.Timestamp.today().normalize()

    if start_dt > end_dt:
        meta = {
            'mode': 'daily_batch',
            'last_date_before': last_date_dt.strftime('%Y%m%d'),
            'rows_after': int(len(old)),
            'symbols_after': int(old['ts_code'].astype(str).nunique()),
            'date_min_after': str(old['trade_date'].min().date()),
            'date_max_after': str(old['trade_date'].max().date()),
            'fetch_errors': 0,
            'trading_days': 0,
            'new_rows_raw': 0,
            'message': 'up-to-date',
        }
        (base / 'refresh_meta.json').write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')
        print(meta)
        return

    sym = pd.read_csv(sym_path)
    if 'ts_code' in sym.columns:
        sym_set = set(_norm_code_no_suffix(sym['ts_code']))
    else:
        sym_set = set(sym['symbol'].astype(str).str.zfill(6))

    pro = _init_pro()

    # trading days between start/end
    cal = pro.trade_cal(start_date=start_dt.strftime('%Y%m%d'), end_date=end_dt.strftime('%Y%m%d'), is_open='1')
    if cal is None or cal.empty:
        trading_days = pd.bdate_range(start_dt, end_dt).strftime('%Y%m%d').tolist()
    else:
        trading_days = sorted(cal['cal_date'].astype(str).tolist())

    frames, errs = [], []
    for i, day in enumerate(trading_days, 1):
        try:
            d = pro.daily(trade_date=day)
            if d is None or d.empty:
                continue
            d['ts_code_n'] = _norm_code_no_suffix(d['ts_code'])
            d = d[d['ts_code_n'].isin(sym_set)].copy()
            if d.empty:
                continue

            x = pd.DataFrame()
            x['trade_date'] = pd.to_datetime(d['trade_date'], format='%Y%m%d')
            x['open'] = d['open']
            x['high'] = d['high']
            x['low'] = d['low']
            x['close'] = d['close']
            x['volume'] = d['vol'] * 100
            x['amount'] = d['amount'] * 1000
            x['ts_code'] = d['ts_code_n']
            x['is_st'] = False
            x['is_suspended'] = False
            x['up_limit'] = x['close'] * 1.1
            x['down_limit'] = x['close'] * 0.9
            x['data_vendor'] = 'tushare_daily'
            frames.append(x)
        except Exception as e:
            errs.append(f'{day}: {type(e).__name__}: {e}')

        if i % 5 == 0 or i == len(trading_days):
            print({'progress_day': i, 'trading_days': len(trading_days), 'new_rows': sum(len(f) for f in frames), 'errs': len(errs)}, flush=True)

    if frames:
        new = pd.concat(frames, ignore_index=True)
        all_df = pd.concat([old, new], ignore_index=True)
        all_df['trade_date'] = pd.to_datetime(all_df['trade_date'])
        all_df = (
            all_df
            .sort_values(['trade_date', 'ts_code'])
            .drop_duplicates(['trade_date', 'ts_code'], keep='last')
            .reset_index(drop=True)
        )
        all_df['trade_date'] = all_df['trade_date'].dt.strftime('%Y-%m-%d')
        all_df.to_parquet(data_path, index=False)
        new_rows_raw = int(len(new))
    else:
        all_df = old.copy()
        all_df['trade_date'] = all_df['trade_date'].dt.strftime('%Y-%m-%d')
        new_rows_raw = 0

    meta = {
        'mode': 'daily_batch',
        'last_date_before': last_date_dt.strftime('%Y%m%d'),
        'rows_after': int(len(all_df)),
        'symbols_after': int(all_df['ts_code'].astype(str).nunique()),
        'date_min_after': str(all_df['trade_date'].min()),
        'date_max_after': str(all_df['trade_date'].max()),
        'fetch_errors': len(errs),
        'trading_days': len(trading_days),
        'new_rows_raw': new_rows_raw,
    }
    (base / 'refresh_meta.json').write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')
    if errs:
        (base / 'refresh_errors.log').write_text('\n'.join(errs), encoding='utf-8')
    print(meta)


if __name__ == '__main__':
    run()
