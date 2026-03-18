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
    x['vol_20'] = g['close'].pct_change().rolling(20).std().reset_index(level=0, drop=True)
    x['amp_5'] = (g['high'].rolling(5).max().reset_index(level=0, drop=True) / g['low'].rolling(5).min().reset_index(level=0, drop=True) - 1)

    for c in ['ret_5', 'ret_20', 'vol_ratio_20', 'amount', 'vol_20', 'amp_5']:
        x[f'cs_{c}'] = cs_rank(x, c)

    # relative label: stock fwd rank minus market median rank proxy (0.5 center)
    x['y_rank'] = cs_rank(x, 'fwd_ret_5') - 0.5
    feats = [f'cs_{c}' for c in ['ret_5', 'ret_20', 'vol_ratio_20', 'amount', 'vol_20', 'amp_5']]
    x = x.dropna(subset=feats + ['y_rank']).copy()
    x['month'] = pd.to_datetime(x['trade_date']).dt.to_period('M').astype(str)
    return x


def monthly_wf_predict(df: pd.DataFrame, feats: list[str], train_months: int = 24) -> pd.DataFrame:
    months = sorted(df['month'].unique().tolist())
    preds = []
    for i in range(train_months, len(months)):
        tr_months = months[i-train_months:i]
        te_month = months[i]
        tr = df[df['month'].isin(tr_months)]
        te = df[df['month'] == te_month]
        if tr.empty or te.empty:
            continue
        model = RandomForestRegressor(
            n_estimators=120,
            max_depth=7,
            min_samples_leaf=80,
            n_jobs=-1,
            random_state=42,
        )
        model.fit(tr[feats], tr['y_rank'])
        out = te[['trade_date', 'ts_code', 'close', 'amount', 'ret_20', 'ma20', 'ma60']].copy()
        out['ml_score'] = model.predict(te[feats])
        preds.append(out)
    if not preds:
        return pd.DataFrame()
    return pd.concat(preds, ignore_index=True)


def bt_from_scores(scored: pd.DataFrame, top_n: int = 8, hold_days: int = 5) -> pd.DataFrame:
    x = scored.sort_values(['ts_code', 'trade_date']).copy()
    x['fwd_ret_1'] = x.groupby('ts_code')['close'].shift(-1) / x['close'] - 1
    dates = sorted(x['trade_date'].unique().tolist())
    h, eq, rows = {}, 1.0, []
    for d in dates:
        day = x[x['trade_date'] == d].copy()
        day = day[(day['amount'] >= 8e8) & (day['close'] >= 3)]
        breadth = float((day['ma20'] > day['ma60']).mean()) if len(day) else 0.0
        risk_on = breadth >= 0.52
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
        rows.append({'trade_date': d, 'daily_ret': net, 'equity': eq, 'risk_on': int(risk_on)})
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

    feats = ['cs_ret_5', 'cs_ret_20', 'cs_vol_ratio_20', 'cs_amount', 'cs_vol_20', 'cs_amp_5']
    scored = monthly_wf_predict(df, feats, train_months=24)
    if scored.empty:
        raise RuntimeError('no wf predictions generated')

    # evaluate only OOS period where wf predictions exist
    curve = bt_from_scores(scored, top_n=8, hold_days=5)
    m = evaluate_curve(curve)
    ok = (m['sharpe'] >= 0.25) and (m['max_drawdown'] >= -0.30) and (m['ann_return'] > 0)

    latest = scored[scored['trade_date'] == scored['trade_date'].max()].copy()
    latest = latest[(latest['amount'] >= 8e8) & (latest['close'] >= 3)].sort_values('ml_score', ascending=False)
    picks = latest[['trade_date', 'ts_code', 'close', 'ml_score', 'ret_20', 'amount']].head(5).copy()
    if len(picks):
        picks['target_weight'] = 1.0 / len(picks)

    out = Path('reports'); out.mkdir(parents=True, exist_ok=True)
    summary = {
        'status': 'wf_ml_candidate_found' if ok else 'wf_ml_no_candidate',
        'oos_metrics': m,
        'oos_days': int(len(curve)),
        'oos_start': str(curve['trade_date'].min()),
        'oos_end': str(curve['trade_date'].max()),
    }
    (out/'stage9_wf_ml_summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    curve.to_parquet(out/'stage9_wf_ml_curve.parquet', index=False)
    if len(picks):
        picks.to_csv(out/'stage9_wf_ml_picks.csv', index=False)

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if len(picks):
        print(picks.to_string(index=False))


if __name__ == '__main__':
    run()
