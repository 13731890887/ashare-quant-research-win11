from __future__ import annotations

from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[3]
AUTH_CFG = ROOT / 'config' / 'auth.yaml'


def load_auth_cfg() -> dict:
    if not AUTH_CFG.exists():
        return {'enabled': False, 'users': []}
    return yaml.safe_load(AUTH_CFG.read_text(encoding='utf-8')) or {'enabled': False, 'users': []}


def verify_user(username: str, password: str) -> bool:
    cfg = load_auth_cfg()
    if not cfg.get('enabled', False):
        return True
    for u in cfg.get('users', []):
        if u.get('username') == username and u.get('password') == password:
            return True
    return False
