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


def prepare(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    x = x.sort_values(['ts_code', 'trade_date'])
    g = x.groupby('ts_code', group_keys=False)
    x['fwd_ret_5'] = g['close'].shift(-5) / x['close'] - 1
    x['vol_20'] = g['close'].pct_change().rolling(20).std().reset_index(level=0, drop=True)
    x['amp_5'] = (g['high'].rolling(5).max().reset_index(level=0, drop=True) / g['low'].rolling(5).min().reset_index(level=0, drop=True) - 1)

    # cross-sectional standardized features by date
    for c in ['ret_5', 'ret_20', 'vol_ratio_20', 'amount', 'vol_20', 'amp_5']:
        x[f'cs_{c}'] = cs_rank(x, c)

    # label: cross-sectional rank of fwd_ret_5 (relative return target)
    x['y_rank'] = cs_rank(x, 'fwd_ret_5')
    feats = [f'cs_{c}' for c in ['ret_5', 'ret_20', 'vol_ratio_20', 'amount', 'vol_20', 'amp_5']]
    x = x.dropna(subset=feats + ['y_rank']).copy()
    return x


def run_bt(df: pd.DataFrame, start: str, end: str, top_n: int = 8, hold_days: int = 5) -> pd.DataFrame:
    x = df[(df['trade_date'] >= start) & (df['trade_date'] <= end)].copy()
    x = x.sort_values(['ts_code', 'trade_date'])
    x['fwd_ret_1'] = x.groupby('ts_code')['close'].shift(-1) / x['close'] - 1
    dates = sorted(x['trade_date'].unique())

    h, eq, rows = {}, 1.0, []
    for d in dates:
        day = x[x['trade_date'] == d].copy()
        day = day[(day['amount'] >= 5e8) & (day['close'] >= 3)]
        day = day.sort_values('ml_score', ascending=False)

        # simple regime filter
        breadth = float((day['ma20'] > day['ma60']).mean()) if len(day) else 0.0
        risk_on = breadth >= 0.5

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

    df = pd.read_parquet(p)
    df = apply_universe_filters(df)
    df = add_basic_factors(df)
    df = prepare(df)

    feats = ['cs_ret_5', 'cs_ret_20', 'cs_vol_ratio_20', 'cs_amount', 'cs_vol_20', 'cs_amp_5']

    train = df[(df['trade_date'] >= '2021-01-04') & (df['trade_date'] <= '2023-12-31')]
    test = df[(df['trade_date'] >= '2024-01-01') & (df['trade_date'] <= '2026-03-13')]

    model = RandomForestRegressor(
        n_estimators=200,
        max_depth=8,
        min_samples_leaf=50,
        n_jobs=-1,
        random_state=42,
    )
    model.fit(train[feats], train['y_rank'])

    pred_all = df.copy()
    pred_all['ml_score'] = model.predict(pred_all[feats])

    tr_curve = run_bt(pred_all, '2021-01-04', '2023-12-31', top_n=8, hold_days=5)
    te_curve = run_bt(pred_all, '2024-01-01', '2026-03-13', top_n=8, hold_days=5)
    mtr, mte = evaluate_curve(tr_curve), evaluate_curve(te_curve)

    # candidate gate
    ok = (mte['sharpe'] >= 0.25) and (mte['max_drawdown'] >= -0.30) and (mte['ann_return'] > 0)

    latest = pred_all[pred_all['trade_date'] == pred_all['trade_date'].max()].copy()
    latest = latest[(latest['amount'] >= 5e8) & (latest['close'] >= 3)].sort_values('ml_score', ascending=False)
    picks = latest[['trade_date', 'ts_code', 'close', 'ml_score', 'ret_20', 'amount']].head(5).copy()
    if len(picks):
        picks['target_weight'] = 1.0 / len(picks)

    out = Path('reports'); out.mkdir(parents=True, exist_ok=True)
    summary = {
        'status': 'ml_candidate_found' if ok else 'ml_no_candidate',
        'train': mtr,
        'test': mte,
        'feature_importance': {f: float(v) for f, v in zip(feats, model.feature_importances_)},
    }
    (out/'stage8_ml_summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    if len(picks):
        picks.to_csv(out/'stage8_ml_picks.csv', index=False)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if len(picks):
        print(picks.to_string(index=False))


if __name__ == '__main__':
    run()
