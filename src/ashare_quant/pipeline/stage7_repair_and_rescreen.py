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


def add_regime_features(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    # market breadth proxy by date
    b = x.groupby('trade_date').apply(lambda g: (g['ma20'] > g['ma60']).mean(), include_groups=False)
    x = x.merge(b.rename('breadth'), on='trade_date', how='left')
    # market momentum proxy from cross-sectional median ret_20
    m = x.groupby('trade_date')['ret_20'].median()
    x = x.merge(m.rename('mkt_mom20'), on='trade_date', how='left')
    return x


def score_family(df: pd.DataFrame, family: str) -> pd.DataFrame:
    x = df.copy()
    liq = cs_rank(x, 'amount')
    qual = ((x['close'] > x['ma20']) & (x['ma20'] > x['ma60'])).astype(int)

    if family == 'trend_quality':
        x['rule_score'] = 0.40 * cs_rank(x, 'ret_20') + 0.20 * cs_rank(x, 'ret_5') + 0.20 * liq + 0.20 * qual
    elif family == 'momentum_conservative':
        x['rule_score'] = 0.55 * cs_rank(x, 'ret_20') + 0.15 * cs_rank(x, 'ret_5') + 0.15 * liq + 0.15 * qual
    elif family == 'breakout_conservative':
        g = x.groupby('ts_code', group_keys=False)
        x['hh55'] = g['high'].transform(lambda s: s.shift(1).rolling(55).max())
        br = (x['close'] >= x['hh55']).fillna(False).astype(int)
        x['rule_score'] = 0.55 * br + 0.20 * cs_rank(x, 'vol_ratio_20') + 0.15 * liq + 0.10 * qual
    else:
        raise ValueError(family)
    return x


def backtest(df: pd.DataFrame, start: str, end: str, top_n: int = 8, hold_days: int = 5) -> pd.DataFrame:
    x = df[(df['trade_date'] >= start) & (df['trade_date'] <= end)].copy()
    x = x.sort_values(['ts_code', 'trade_date'])
    x['fwd_ret_1'] = x.groupby('ts_code')['close'].shift(-1) / x['close'] - 1
    dates = sorted(x['trade_date'].unique())

    h, eq, rows = {}, 1.0, []
    for d in dates:
        day = x[x['trade_date'] == d].copy()
        # execution filters
        day = day[(day['amount'] >= 5e8) & (day['close'] >= 3)]
        if 'is_limit_up_close' in day.columns:
            day = day[~day['is_limit_up_close']]

        day = day.sort_values('rule_score', ascending=False)

        # regime -> cash mode
        breadth = float(day['breadth'].iloc[0]) if len(day) else 0.0
        mkt_mom = float(day['mkt_mom20'].iloc[0]) if len(day) else -1.0
        risk_on = (breadth >= 0.52) and (mkt_mom > -0.01)

        rr = []
        if h:
            k = day.set_index('ts_code')
            rr = [float(k.loc[c, 'fwd_ret_1']) for c in h if c in k.index and pd.notna(k.loc[c, 'fwd_ret_1'])]
        dr = float(np.mean(rr)) if rr else 0.0

        sell = 0
        for c in list(h.keys()):
            h[c] += 1
            # soft stop-loss/take-profit proxy on daily move
            if h[c] >= hold_days:
                del h[c]
                sell += 1

        buy = 0
        if risk_on:
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
        rows.append({'trade_date': d, 'daily_ret': net, 'equity': eq, 'risk_on': int(risk_on), 'breadth': breadth, 'mkt_mom20': mkt_mom})
    return pd.DataFrame(rows)


def latest_picks(df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    d = df['trade_date'].max()
    day = df[df['trade_date'] == d].copy()
    day = day[(day['amount'] >= 5e8) & (day['close'] >= 3)]
    if 'is_limit_up_close' in day.columns:
        day = day[~day['is_limit_up_close']]
    day = day.sort_values('rule_score', ascending=False)
    picks = day[['trade_date', 'ts_code', 'close', 'rule_score', 'ret_20', 'amount']].head(top_n).copy()
    if len(picks):
        picks['target_weight'] = 1.0 / len(picks)
    return picks


def run() -> None:
    cfg = load_config('configs/research.yaml')
    p = Path(cfg.data.all_buyable_path)
    if not p.exists():
        raise FileNotFoundError(p)

    df = pd.read_parquet(p)
    df = apply_universe_filters(df)
    df = add_basic_factors(df)
    df = add_regime_features(df)

    fams = ['trend_quality', 'momentum_conservative', 'breakout_conservative']
    rows, scored = [], {}
    for fam in fams:
        s = score_family(df, fam)
        scored[fam] = s
        tr = backtest(s, '2021-01-04', '2023-12-31', top_n=8, hold_days=5)
        te = backtest(s, '2024-01-01', '2026-03-13', top_n=8, hold_days=5)
        rows.append({'family': fam, 'train': evaluate_curve(tr), 'test': evaluate_curve(te), 'risk_on_ratio_test': float(te['risk_on'].mean()) if len(te) else 0.0})

    ranked = sorted(rows, key=lambda r: (r['test']['sharpe'], r['test']['ann_return']), reverse=True)
    cands, elim = [], []
    for r in ranked:
        t = r['test']
        reasons = []
        if t['sharpe'] < 0.25: reasons.append('sharpe<0.25')
        if t['max_drawdown'] < -0.30: reasons.append('mdd<-30%')
        if t['ann_return'] <= 0: reasons.append('ann<=0')
        if reasons: elim.append({'family': r['family'], 'test': t, 'reasons': reasons})
        else: cands.append(r)

    out = Path('reports'); out.mkdir(parents=True, exist_ok=True)
    (out/'stage7_repair_eval.json').write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding='utf-8')
    (out/'stage7_repair_candidates.json').write_text(json.dumps(cands, ensure_ascii=False, indent=2), encoding='utf-8')
    (out/'stage7_repair_eliminated.json').write_text(json.dumps(elim, ensure_ascii=False, indent=2), encoding='utf-8')

    if cands:
        main = cands[0]['family']
        picks = latest_picks(scored[main], top_n=5)
        picks.to_csv(out/'stage7_main_picks.csv', index=False)
        summary = {'status':'candidate_found','main_family':main,'main_test':cands[0]['test'],'risk_on_ratio_test':cands[0]['risk_on_ratio_test'],'n_picks':int(len(picks)),'trade_date':str(picks['trade_date'].iloc[0]) if len(picks) else None}
    else:
        summary = {'status':'no_candidate_after_repair','top_family':ranked[0]['family'],'top_test':ranked[0]['test']}

    (out/'stage7_repair_summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if cands and len(picks):
        print(picks.to_string(index=False))


if __name__ == '__main__':
    run()
