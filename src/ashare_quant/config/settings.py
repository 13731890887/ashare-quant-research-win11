from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class CostConfig:
    commission_bps: float = 2.5
    stamp_duty_sell_bps: float = 10.0
    slippage_bps: float = 5.0

@dataclass(frozen=True)
class TradeConstraints:
    t_plus_1: bool = True
    lot_size: int = 100
    max_participation_rate: float = 0.1

@dataclass(frozen=True)
class UniverseFilter:
    min_turnover_million: float = 50.0
    exclude_st: bool = True
    exclude_suspended: bool = True

@dataclass(frozen=True)
class Settings:
    costs: CostConfig = CostConfig()
    constraints: TradeConstraints = TradeConstraints()
    universe: UniverseFilter = UniverseFilter()

SETTINGS = Settings()
