import json
from pathlib import Path

_DIR  = Path(__file__).parent / "data"
_FILE = _DIR / "subscribers.json"
_DIR.mkdir(exist_ok=True)

def _load():
    if _FILE.exists():
        return json.loads(_FILE.read_text(encoding="utf-8"))
    return {"users": [], "chats": []}

def _save(d):
    _FILE.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")

def subscribe(chat_id, is_private):
    d = _load(); k = "users" if is_private else "chats"
    if chat_id not in d[k]:
        d[k].append(chat_id); _save(d); return True
    return False

def unsubscribe(chat_id, is_private):
    d = _load(); k = "users" if is_private else "chats"
    if chat_id in d[k]:
        d[k].remove(chat_id); _save(d); return True
    return False

def is_subscribed(chat_id, is_private):
    d = _load(); k = "users" if is_private else "chats"
    return chat_id in d[k]

def toggle(chat_id, is_private):
    if is_subscribed(chat_id, is_private):
        unsubscribe(chat_id, is_private); return False
    subscribe(chat_id, is_private); return True

def all_subscribers():
    d = _load(); return d["users"] + d["chats"]

def stats():
    d = _load(); return len(d["users"]), len(d["chats"])
