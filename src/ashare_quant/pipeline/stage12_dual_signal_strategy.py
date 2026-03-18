from __future__ import annotations

from pathlib import Path
import json
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor

from ashare_quant.config.research_config import load_config
from ashare_quant.universe.filtering import apply_universe_filters
from ashare_quant.factors.price_volume import add_basic_factors
from ashare_quant.utils.ranking import cs_rank


def prepare(df: pd.DataFrame) -> pd.DataFrame:
    x = df.sort_values(['ts_code', 'trade_date']).copy()
    g = x.groupby('ts_code', group_keys=False)
    x['fwd_ret_5'] = g['close'].shift(-5) / x['close'] - 1
    # risk label: forward 5d worst draw proxy (negative return magnitude)
    x['fwd_min5'] = g['low'].transform(lambda s: s.shift(-1).rolling(5).min())
    x['fwd_dd5'] = (x['fwd_min5'] / x['close'] - 1).fillna(0)

    x['vol_20'] = g['close'].transform(lambda s: s.pct_change().rolling(20).std())
    x['amp_5'] = g['high'].transform(lambda s: s.rolling(5).max()) / g['low'].transform(lambda s: s.rolling(5).min()) - 1

    for c in ['ret_5', 'ret_20', 'vol_ratio_20', 'amount', 'vol_20', 'amp_5']:
        x[f'cs_{c}'] = cs_rank(x, c)

    # normalized labels
    x['up_score_label'] = cs_rank(x, 'fwd_ret_5')
    # risk higher means worse drawdown -> rank of drawdown magnitude
    x['dd_mag_5'] = (-x['fwd_dd5']).clip(lower=0)
    x['risk_score_label'] = cs_rank(x, 'dd_mag_5')

    feats = ['cs_ret_5', 'cs_ret_20', 'cs_vol_ratio_20', 'cs_amount', 'cs_vol_20', 'cs_amp_5']
    x = x.dropna(subset=feats + ['up_score_label', 'risk_score_label']).copy()
    return x


def fit_predict(train: pd.DataFrame, pred: pd.DataFrame, feats: list[str]) -> pd.DataFrame:
    up = RandomForestRegressor(n_estimators=140, max_depth=8, min_samples_leaf=80, random_state=42, n_jobs=-1)
    rk = RandomForestRegressor(n_estimators=140, max_depth=8, min_samples_leaf=80, random_state=43, n_jobs=-1)
    up.fit(train[feats], train['up_score_label'])
    rk.fit(train[feats], train['risk_score_label'])

    out = pred[['trade_date', 'ts_code', 'close', 'amount', 'ret_20', 'ma20', 'ma60']].copy()
    out['up_score'] = up.predict(pred[feats])
    out['risk_score'] = rk.predict(pred[feats])
    return out


def generate_actions(latest: pd.DataFrame, current_holdings: set[str], top_n: int = 5) -> dict:
    d = latest.copy()
    d = d[(d['amount'] >= 8e8) & (d['close'] >= 5)]
    d = d[(d['up_score'] >= 0.46) & (d['risk_score'] <= 0.30)]
    d = d.sort_values(['up_score', 'risk_score'], ascending=[False, True])
    buy_list = d['ts_code'].head(top_n).tolist()

    hold = []
    sell = []
    table = latest.set_index('ts_code')
    for c in sorted(current_holdings):
        if c not in table.index:
            sell.append({'ts_code': c, 'reason': 'missing_today'})
            continue
        row = table.loc[c]
        up = float(row['up_score'])
        rk = float(row['risk_score'])
        if (rk >= 0.65) or (up < 0.45):
            sell.append({'ts_code': c, 'reason': f'risk_or_weak_signal(up={up:.3f},risk={rk:.3f})'})
        else:
            hold.append({'ts_code': c, 'up_score': up, 'risk_score': rk})

    # remove already-held from buy list
    buy = [c for c in buy_list if c not in current_holdings]
    return {'buy': buy, 'hold': hold, 'sell': sell}


def run() -> None:
    cfg = load_config('configs/research.yaml')
    p = Path(cfg.data.all_buyable_path)
    if not p.exists():
        raise FileNotFoundError(p)

    raw = pd.read_parquet(p)
    raw = apply_universe_filters(raw)
    raw = add_basic_factors(raw)
    df = prepare(raw)
    feats = ['cs_ret_5', 'cs_ret_20', 'cs_vol_ratio_20', 'cs_amount', 'cs_vol_20', 'cs_amp_5']

    train = df[(df['trade_date'] >= '2021-01-04') & (df['trade_date'] <= '2023-12-31')]
    pred_end = str(df['trade_date'].max())
    pred = df[(df['trade_date'] >= '2024-01-01') & (df['trade_date'] <= pred_end)]
    scored = fit_predict(train, pred, feats)

    latest_date = scored['trade_date'].max()
    latest = scored[scored['trade_date'] == latest_date].copy()

    holdings_file = Path('reports/current_holdings.csv')
    if holdings_file.exists():
        hdf = pd.read_csv(holdings_file)
        current_holdings = set(hdf['ts_code'].astype(str).str.zfill(6).tolist())
    else:
        current_holdings = set()

    actions = generate_actions(latest, current_holdings, top_n=5)

    out = Path('reports'); out.mkdir(parents=True, exist_ok=True)
    latest.sort_values(['up_score', 'risk_score'], ascending=[False, True]).head(50).to_csv(out/'stage12_dual_top50.csv', index=False)
    (out/'stage12_actions.json').write_text(json.dumps({'trade_date': str(latest_date), **actions}, ensure_ascii=False, indent=2), encoding='utf-8')

    summary = {
        'trade_date': str(latest_date),
        'n_candidates_top50': 50,
        'buy_count': len(actions['buy']),
        'hold_count': len(actions['hold']),
        'sell_count': len(actions['sell']),
        'rule': {'buy': 'up>=0.46 & risk<=0.30', 'sell': 'risk>=0.65 or up<0.45'}
    }
    (out/'stage12_summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(json.dumps(actions, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    run()
