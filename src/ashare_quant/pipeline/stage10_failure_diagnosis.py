from __future__ import annotations

from pathlib import Path
import json
import pandas as pd


def run() -> None:
    curve_p = Path('reports/stage9_wf_ml_curve.parquet')
    data_p = Path('data/stage4_all_buyable/market_daily_all_buyable_20210101_20260314.parquet')
    if not curve_p.exists() or not data_p.exists():
        raise FileNotFoundError('required inputs missing')

    curve = pd.read_parquet(curve_p).sort_values('trade_date').reset_index(drop=True)
    mkt = pd.read_parquet(data_p, columns=['trade_date', 'ts_code', 'close', 'amount']).copy()
    mkt = mkt.sort_values(['ts_code', 'trade_date'])
    mkt['ret_1_proxy'] = mkt.groupby('ts_code')['close'].pct_change()
    mkt['ret_20_proxy'] = mkt.groupby('ts_code')['close'].pct_change(20)

    mkt_day = mkt.groupby('trade_date').agg(
        mkt_ret=('ret_1_proxy', 'median'),
        mkt_mom20=('ret_20_proxy', 'median'),
        mkt_liq=('amount', 'median')
    ).reset_index()

    z = curve.merge(mkt_day, on='trade_date', how='left')
    z['excess_ret'] = z['daily_ret'] - z['mkt_ret'].fillna(0)
    z['drawdown'] = z['equity'] / z['equity'].cummax() - 1

    bins = [-1, -0.08, 0.0, 1]
    labels = ['weak', 'neutral', 'strong']
    z['phase'] = pd.cut(z['mkt_mom20'].fillna(0), bins=bins, labels=labels, include_lowest=True)
    by_phase = z.groupby('phase').agg(
        days=('daily_ret', 'count'),
        mean_ret=('daily_ret', 'mean'),
        mean_excess=('excess_ret', 'mean'),
        win_rate=('daily_ret', lambda s: float((s > 0).mean())),
    ).reset_index()

    by_risk = z.groupby('risk_on').agg(
        days=('daily_ret', 'count'),
        mean_ret=('daily_ret', 'mean'),
        win_rate=('daily_ret', lambda s: float((s > 0).mean())),
    ).reset_index() if 'risk_on' in z.columns else pd.DataFrame()

    worst20 = z.nsmallest(20, 'daily_ret')[['trade_date', 'daily_ret', 'mkt_ret', 'excess_ret', 'mkt_mom20']]

    diagnosis = {
        'oos_days': int(len(z)),
        'mean_daily_ret': float(z['daily_ret'].mean()),
        'mean_excess_ret': float(z['excess_ret'].mean()),
        'win_rate': float((z['daily_ret'] > 0).mean()),
        'max_drawdown': float(z['drawdown'].min()),
        'phase_summary': by_phase.to_dict(orient='records'),
        'risk_on_summary': by_risk.to_dict(orient='records') if not by_risk.empty else [],
        'likely_failure_causes': [
            'ML ranking has no stable OOS alpha under current features',
            'risk_on filter not sufficient to avoid weak-regime bleed',
            'model may be over-exposed to liquidity/size effects',
            '5-day label/execution mismatch under cost constraints',
        ],
        'next_repairs': [
            'use residual label vs market + size bucket benchmark',
            'purged WF with embargo and monthly calibration',
            'explicit neutralization by liquidity/size buckets',
            'tighter no-trade regime with volatility gate',
        ],
    }

    out = Path('reports'); out.mkdir(parents=True, exist_ok=True)
    (out/'stage10_diagnosis_summary.json').write_text(json.dumps(diagnosis, ensure_ascii=False, indent=2), encoding='utf-8')
    worst20.to_csv(out/'stage10_worst20_days.csv', index=False)

    print(json.dumps(diagnosis, ensure_ascii=False, indent=2))
    print('worst20_sample:')
    print(worst20.head(10).to_string(index=False))


if __name__ == '__main__':
    run()
