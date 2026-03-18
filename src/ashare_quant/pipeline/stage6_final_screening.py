from __future__ import annotations

from pathlib import Path
import json
import numpy as np
import pandas as pd

from ashare_quant.config.research_config import load_config
from ashare_quant.universe.filtering import apply_universe_filters
from ashare_quant.factors.price_volume import add_basic_factors
from ashare_quant.reporting.metrics import evaluate_curve
from ashare_quant.utils.ranking import cs_rank


def score(df: pd.DataFrame, family: str) -> pd.DataFrame:
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


def bt(df: pd.DataFrame, start: str, end: str, top_n: int, hold_days: int) -> pd.DataFrame:
    x = df[(df['trade_date'] >= start) & (df['trade_date'] <= end)].copy()
    x = x.sort_values(['ts_code', 'trade_date'])
    x['fwd_ret_1'] = x.groupby('ts_code')['close'].shift(-1) / x['close'] - 1
    dates = sorted(x['trade_date'].unique())

    h, eq, rows = {}, 1.0, []
    for d in dates:
        day = x[x['trade_date'] == d].copy()
        # tradability guard: avoid extreme low amount and limit-up close locks
        day = day[day['amount'] >= 2e8]
        day = day[~((day.get('is_limit_up_close', False) == True))] if 'is_limit_up_close' in day.columns else day
        day = day.sort_values('rule_score', ascending=False)

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

        # regime filter: if market breadth weak, skip new buys (cash)
        breadth = float((day['ma20'] > day['ma60']).mean()) if len(day) else 0.0
        buy = 0
        if breadth >= 0.45:
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
        rows.append({'trade_date': d, 'daily_ret': net, 'equity': eq, 'breadth': breadth, 'n_hold': len(h)})
    return pd.DataFrame(rows)


def latest_picks(df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    d = df['trade_date'].max()
    day = df[df['trade_date'] == d].copy()
    day = day[day['amount'] >= 2e8]
    day = day.sort_values('rule_score', ascending=False)
    out = day[['trade_date', 'ts_code', 'close', 'rule_score', 'ret_20', 'amount']].head(top_n).copy()
    out['target_weight'] = 1.0 / max(len(out), 1)
    return out


def run() -> None:
    cfg = load_config('configs/research.yaml')
    p = Path(cfg.data.all_buyable_path)
    if not p.exists():
        raise FileNotFoundError(p)

    df = pd.read_parquet(p)
    df = apply_universe_filters(df)
    df = add_basic_factors(df)

    families = ['momentum', 'trend', 'breakout']
    results = []
    scored = {}

    for fam in families:
        s = score(df, fam)
        scored[fam] = s
        tr = bt(s, '2021-01-04', '2023-12-31', cfg.backtest.top_n, cfg.backtest.hold_days)
        te = bt(s, '2024-01-01', '2026-03-13', cfg.backtest.top_n, cfg.backtest.hold_days)
        mtr, mte = evaluate_curve(tr), evaluate_curve(te)
        results.append({'family': fam, 'train': mtr, 'test': mte})

    ranked = sorted(results, key=lambda r: (r['test']['sharpe'], r['test']['ann_return'], r['test']['max_drawdown']), reverse=True)

    # strict gate
    candidates = []
    eliminated = []
    for r in ranked:
        t = r['test']
        reasons = []
        if t['sharpe'] < 0.3: reasons.append('sharpe<0.3')
        if t['max_drawdown'] < -0.35: reasons.append('mdd<-35%')
        if t['ann_return'] <= 0: reasons.append('ann<=0')
        if reasons:
            eliminated.append({'family': r['family'], 'test': t, 'reasons': reasons})
        else:
            candidates.append(r)

    out = Path('reports')
    out.mkdir(parents=True, exist_ok=True)
    (out/'stage6_eval.json').write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
    (out/'stage6_candidates.json').write_text(json.dumps(candidates, ensure_ascii=False, indent=2), encoding='utf-8')
    (out/'stage6_eliminated.json').write_text(json.dumps(eliminated, ensure_ascii=False, indent=2), encoding='utf-8')

    if candidates:
        main = candidates[0]['family']
        picks = latest_picks(scored[main], top_n=5)
        picks.to_csv(out/'stage6_main_picks.csv', index=False)
        summary = {'status': 'tradable_candidate_found', 'main_family': main, 'main_test': candidates[0]['test'], 'n_picks': int(len(picks)), 'trade_date': str(picks['trade_date'].iloc[0])}
    else:
        summary = {'status': 'no_tradable_candidate', 'message': 'all families eliminated by strict gate', 'top_family': ranked[0]['family'], 'top_test': ranked[0]['test']}

    (out/'stage6_summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if candidates:
        print(picks.to_string(index=False))


if __name__ == '__main__':
    run()
