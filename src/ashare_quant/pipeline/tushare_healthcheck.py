from __future__ import annotations
from ashare_quant.data.tushare_loader import health_check_tushare


def run() -> None:
    print(health_check_tushare())


if __name__ == "__main__":
    run()
