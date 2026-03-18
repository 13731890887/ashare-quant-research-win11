from __future__ import annotations

from pathlib import Path
import json
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor

from ashare_quant.config.research_config import load_config
from ashare_quant.universe.filtering import apply_universe_filters
from ashare_quant.factors.price_volume import add_basic_factors
from ashare_quant.reporting.metrics import evaluate_curve
from ashare_quant.utils.ranking import cs_rank


def prep(df: pd.DataFrame) -> pd.DataFrame:
    x = df.sort_values(['ts_code', 'trade_date']).copy()
    g = x.groupby('ts_code', group_keys=False)
    x['fwd_ret_5'] = g['close'].shift(-5) / x['close'] - 1
    x['vol_20'] = g['close'].transform(lambda s: s.pct_change().rolling(20).std())
    x['turnover_20'] = g['amount'].transform(lambda s: s.rolling(20).mean())

    # cross-sectional features
    for c in ['ret_5', 'ret_20', 'vol_ratio_20', 'amount', 'vol_20', 'turnover_20']:
        x[f'cs_{c}'] = cs_rank(x, c)

    # liquidity bucket neutral residual label
    x['liq_bucket'] = (x.groupby('trade_date')['amount'].rank(pct=True).fillna(0) * 10).clip(0,9).astype(int)
    bucket_mean = x.groupby(['trade_date', 'liq_bucket'])['fwd_ret_5'].transform('mean')
    x['label_resid'] = x['fwd_ret_5'] - bucket_mean

    x['month'] = pd.to_datetime(x['trade_date']).dt.to_period('M').astype(str)
    feats = ['cs_ret_5', 'cs_ret_20', 'cs_vol_ratio_20', 'cs_amount', 'cs_vol_20', 'cs_turnover_20']
    x = x.dropna(subset=feats + ['label_resid']).copy()
    return x


def monthly_wf(df: pd.DataFrame, feats: list[str], train_months: int = 24, embargo_days: int = 5) -> pd.DataFrame:
    months = sorted(df['month'].unique().tolist())
    out = []
    for i in range(train_months, len(months)):
        te_month = months[i]
        tr_months = months[i-train_months:i]
        tr = df[df['month'].isin(tr_months)].copy()
        te = df[df['month'] == te_month].copy()
        if tr.empty or te.empty:
            continue

        # purged + embargo: remove train rows too close to test start
        te_start = pd.to_datetime(te['trade_date']).min()
        tr = tr[pd.to_datetime(tr['trade_date']) < (te_start - pd.Timedelta(days=embargo_days))]
        if tr.empty:
            continue

        m = RandomForestRegressor(
            n_estimators=160,
            max_depth=7,
            min_samples_leaf=120,
            n_jobs=-1,
            random_state=42,
        )
        m.fit(tr[feats], tr['label_resid'])
        p = te[['trade_date', 'ts_code', 'close', 'amount', 'ret_20', 'ma20', 'ma60', 'vol_20']].copy()
        p['ml_score'] = m.predict(te[feats])
        out.append(p)

    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def backtest(scored: pd.DataFrame, top_n: int = 6, hold_days: int = 5) -> pd.DataFrame:
    x = scored.sort_values(['ts_code', 'trade_date']).copy()
    x['fwd_ret_1'] = x.groupby('ts_code')['close'].shift(-1) / x['close'] - 1
    x['vol_gate'] = x.groupby('trade_date')['vol_20'].transform('median')
    dates = sorted(x['trade_date'].unique().tolist())

    h, eq, rows = {}, 1.0, []
    for d in dates:
        day = x[x['trade_date'] == d].copy()
        # tradability + anti-microcap + low volatility preference
        day = day[(day['amount'] >= 1e9) & (day['close'] >= 5)]
        day = day[day['vol_20'] <= day['vol_gate'] * 1.2]

        breadth = float((day['ma20'] > day['ma60']).mean()) if len(day) else 0.0
        # tighter no-trade regime
        risk_on = breadth >= 0.58

        day = day.sort_values('ml_score', ascending=False)

        rr = []
        if h:
            k = day.set_index('ts_code')
            rr = [float(k.loc[c, 'fwd_ret_1']) for c in h if c in k.index and pd.notna(k.loc[c, 'fwd_ret_1'])]
        dr = float(np.mean(rr)) if rr else 0.0

        sell = 0
        for c in list(h.keys()):
            h[c] += 1
            if h[c] >= hold_days:
                del h[c]; sell += 1

        buy = 0
        if risk_on:
            slots = max(0, top_n - len(h))
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
        rows.append({'trade_date': d, 'daily_ret': net, 'equity': eq, 'risk_on': int(risk_on), 'n_hold': len(h)})

    return pd.DataFrame(rows)


def run() -> None:
    cfg = load_config('configs/research.yaml')
    p = Path(cfg.data.all_buyable_path)
    if not p.exists():
        raise FileNotFoundError(p)

    raw = pd.read_parquet(p)
    raw = apply_universe_filters(raw)
    raw = add_basic_factors(raw)
    df = prep(raw)

    feats = ['cs_ret_5', 'cs_ret_20', 'cs_vol_ratio_20', 'cs_amount', 'cs_vol_20', 'cs_turnover_20']
    scored = monthly_wf(df, feats, train_months=24, embargo_days=5)
    if scored.empty:
        raise RuntimeError('no predictions in stage11')

    # OOS only where predictions exist
    curve = backtest(scored, top_n=12, hold_days=40)
    m = evaluate_curve(curve)

    ok = (m['sharpe'] >= 0.25) and (m['max_drawdown'] >= -0.25) and (m['ann_return'] > 0)

    latest = scored[scored['trade_date'] == scored['trade_date'].max()].copy()
    latest = latest[(latest['amount'] >= 1e9) & (latest['close'] >= 5)]
    latest = latest.sort_values('ml_score', ascending=False)
    picks = latest[['trade_date', 'ts_code', 'close', 'ml_score', 'ret_20', 'amount']].head(5).copy()
    if len(picks):
        picks['target_weight'] = 1.0 / len(picks)

    out = Path('reports'); out.mkdir(parents=True, exist_ok=True)
    summary = {
        'status': 'stage11_candidate_found' if ok else 'stage11_no_candidate',
        'oos_metrics': m,
        'oos_days': int(len(curve)),
        'oos_start': str(curve['trade_date'].min()),
        'oos_end': str(curve['trade_date'].max()),
        'risk_on_ratio': float(curve['risk_on'].mean()) if len(curve) else 0.0,
    }
    (out/'stage11_summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    curve.to_parquet(out/'stage11_curve.parquet', index=False)
    if len(picks):
        picks.to_csv(out/'stage11_picks.csv', index=False)

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if len(picks):
        print(picks.to_string(index=False))


if __name__ == '__main__':
    run()
