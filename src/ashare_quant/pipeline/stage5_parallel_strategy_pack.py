from __future__ import annotations

from pathlib import Path
import json
import numpy as np
import pandas as pd

from ashare_quant.universe.filtering import apply_universe_filters
from ashare_quant.factors.price_volume import add_basic_factors
from ashare_quant.reporting.metrics import evaluate_curve
from ashare_quant.utils.ranking import cs_rank


def score_family(df: pd.DataFrame, family: str) -> pd.DataFrame:
    x = df.copy()
    if family == 'momentum':
        x['rule_score'] = cs_rank(x, 'ret_20') * 0.7 + cs_rank(x, 'ret_5') * 0.3
    elif family == 'trend':
        x['rule_score'] = (x['ma20'] > x['ma60']).astype(int) * 0.45 + cs_rank(x, 'ret_20') * 0.35 + cs_rank(x, 'amount') * 0.2
    elif family == 'breakout':
        g = x.groupby('ts_code', group_keys=False)
        x['hh55'] = g['high'].transform(lambda s: s.shift(1).rolling(55).max())
        x['rule_score'] = (x['close'] >= x['hh55']).fillna(False).astype(int) * 0.7 + cs_rank(x, 'vol_ratio_20') * 0.3
    else:
        raise ValueError(family)
    return x


def bt(df: pd.DataFrame, start: str, end: str, top_n: int = 12, hold_days: int = 5) -> pd.DataFrame:
    x = df[(df['trade_date'] >= start) & (df['trade_date'] <= end)].copy()
    x = x.sort_values(['ts_code', 'trade_date'])
    x['fwd_ret_1'] = x.groupby('ts_code')['close'].shift(-1) / x['close'] - 1
    dates = sorted(x['trade_date'].unique())
    h, eq, rows = {}, 1.0, []
    for d in dates:
        day = x[x['trade_date'] == d].sort_values('rule_score', ascending=False)
        rr = []
        if h:
            k = day.set_index('ts_code')
            rr = [float(k.loc[c, 'fwd_ret_1']) for c in h if c in k.index and pd.notna(k.loc[c, 'fwd_ret_1'])]
        dr = float(np.mean(rr)) if rr else 0.0

        sell = 0
        for c in list(h.keys()):
            h[c] += 1
            if h[c] >= hold_days:
                del h[c]
                sell += 1

        buy = 0
        slots = max(0, top_n - len(h))
        if slots > 0:
            for _, r in day.iterrows():
                c = r['ts_code']
                if c in h:
                    continue
                h[c] = 0
                buy += 1
                slots -= 1
                if slots <= 0:
                    break

        trade_frac = (buy + sell) / max(top_n, 1)
        net = dr - trade_frac * (2.5 + 5.0) / 10000 - (sell / max(top_n, 1)) * 10 / 10000
        eq *= (1 + net)
        rows.append({'trade_date': d, 'daily_ret': net, 'equity': eq})
    return pd.DataFrame(rows)


def latest_picks(df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    d = df['trade_date'].max()
    day = df[df['trade_date'] == d].sort_values('rule_score', ascending=False)
    picks = day[['trade_date', 'ts_code', 'close', 'rule_score', 'ret_20', 'amount']].head(top_n).copy()
    picks['target_weight'] = 1.0 / max(len(picks), 1)
    return picks


def run() -> None:
    p = Path('data/stage4_all_buyable/market_daily_all_buyable_20210101_20260314.parquet')
    if not p.exists():
        raise FileNotFoundError(p)
    df = pd.read_parquet(p)

    df = apply_universe_filters(df)
    df = add_basic_factors(df)

    families = ['momentum', 'trend', 'breakout']
    rows, pick_map = [], {}

    # split: train 2021-2023, test 2024-2026
    for fam in families:
        s = score_family(df, fam)
        tr = bt(s, '2021-01-04', '2023-12-31', top_n=12, hold_days=5)
        te = bt(s, '2024-01-01', '2026-03-13', top_n=12, hold_days=5)
        mtr, mte = evaluate_curve(tr), evaluate_curve(te)
        rows.append({'family': fam, 'train': mtr, 'test': mte})
        pick_map[fam] = latest_picks(s, top_n=5)

    ranked = sorted(rows, key=lambda r: (r['test']['sharpe'], r['test']['ann_return']), reverse=True)
    main = ranked[0]['family']
    alt = ranked[1]['family'] if len(ranked) > 1 else ranked[0]['family']

    out = Path('reports')
    out.mkdir(parents=True, exist_ok=True)
    (out / 'stage5_family_pack_eval.json').write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding='utf-8')
    pick_map[main].to_csv(out / 'stage5_main_picks.csv', index=False)
    pick_map[alt].to_csv(out / 'stage5_alt_picks.csv', index=False)

    plan = {
        'main_family': main,
        'alt_family': alt,
        'main_test': [r['test'] for r in ranked if r['family'] == main][0],
        'alt_test': [r['test'] for r in ranked if r['family'] == alt][0],
        'risk_rules': {
            'single_position_weight': 0.20,
            'max_positions': 5,
            'stop_loss': -0.07,
            'take_profit_partial': 0.12,
            'rebalance_cycle_days': 5,
        }
    }
    (out / 'stage5_execution_plan.json').write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(plan, ensure_ascii=False, indent=2))
    print('main picks:')
    print(pick_map[main].to_string(index=False))
    print('alt picks:')
    print(pick_map[alt].to_string(index=False))


if __name__ == '__main__':
    run()
