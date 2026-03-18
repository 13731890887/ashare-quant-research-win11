from __future__ import annotations

from pathlib import Path
import yaml
import requests

ROOT = Path(__file__).resolve().parents[3]
CFG = ROOT / 'config' / 'notify.yaml'


def load_notify_cfg() -> dict:
    if not CFG.exists():
        return {'enabled': False, 'webhook_url': ''}
    return yaml.safe_load(CFG.read_text(encoding='utf-8')) or {'enabled': False, 'webhook_url': ''}


def save_notify_cfg(cfg: dict) -> None:
    CFG.parent.mkdir(parents=True, exist_ok=True)
    CFG.write_text(yaml.safe_dump(cfg, allow_unicode=True, sort_keys=False), encoding='utf-8')


def send_webhook(msg: str) -> tuple[bool, str]:
    cfg = load_notify_cfg()
    if not cfg.get('enabled'):
        return False, 'notify disabled'
    url = cfg.get('webhook_url', '').strip()
    if not url:
        return False, 'webhook url empty'
    try:
        r = requests.post(url, json={'text': msg}, timeout=8)
        return (r.status_code < 400), f'status={r.status_code}'
    except Exception as e:
        return False, str(e)
