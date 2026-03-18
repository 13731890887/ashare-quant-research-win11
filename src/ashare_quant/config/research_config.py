from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import yaml

@dataclass(frozen=True)
class DataCfg:
    all_buyable_path: str
    start_date: str
    end_date: str

@dataclass(frozen=True)
class BacktestCfg:
    top_n: int
    hold_days: int
    commission_bps: float
    slippage_bps: float
    stamp_duty_sell_bps: float

@dataclass(frozen=True)
class RiskCfg:
    stop_loss: float
    take_profit_partial: float
    max_positions: int

@dataclass(frozen=True)
class ResearchCfg:
    data: DataCfg
    backtest: BacktestCfg
    risk: RiskCfg


def load_config(path: str = 'configs/research.yaml') -> ResearchCfg:
    p = Path(path)
    d = yaml.safe_load(p.read_text(encoding='utf-8'))
    return ResearchCfg(
        data=DataCfg(**d['data']),
        backtest=BacktestCfg(**d['backtest']),
        risk=RiskCfg(**d['risk']),
    )
