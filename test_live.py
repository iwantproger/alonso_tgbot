"""
F1 Bot — тест live-монитора на реальных исторических данных OpenF1.

Использование:
  python3 test_live.py                        # консольный вывод, последняя гонка
  python3 test_live.py --sk 9574              # конкретный session_key
  python3 test_live.py --token TOK --chat ID  # слать в Telegram
  python3 test_live.py --speed 50             # скорость воспроизведения (×50)
  python3 test_live.py --list                 # список последних сессий OpenF1

Режимы:
  --dry-run   только консоль (по умолчанию)
  --telegram  слать сообщения в Telegram-чат

Примеры конкретных session_key из OpenF1:
  9574 — Australia 2025 Race
  9161 — Abu Dhabi 2024 Race
  9149 — Brazil 2024 Race (красный флаг!)
  9527 — Bahrain 2025 Race
"""

import asyncio
import argparse
import re
import sys
from datetime import datetime, timezone
from html.parser import HTMLParser

import aiohttp

# ─── Strip HTML tags for console output ──────────────────────────────────────
class _StripHTML(HTMLParser):
    def __init__(self):
        super().__init__()
        self._parts = []
    def handle_data(self, data):
        self._parts.append(data)
    def get(self):
        return "".join(self._parts)

def strip_html(text: str) -> str:
    p = _StripHTML()
    p.feed(text)
    return p.get()

# ─── Config ───────────────────────────────────────────────────────────────────
O  = "https://api.openf1.org/v1"
MV = "https://api.multiviewer.app/api/v1"
TIMEOUT = aiohttp.ClientTimeout(total=10)

# Флаги гонщиков
_DRIVER_FLAG = {
    "Verstappen":"🇳🇱","Hamilton":"🇬🇧","Leclerc":"🇲🇨","Norris":"🇬🇧",
    "Piastri":"🇦🇺","Russell":"🇬🇧","Sainz":"🇪🇸","Alonso":"🇪🇸",
    "Perez":"🇲🇽","Stroll":"🇨🇦","Gasly":"🇫🇷","Ocon":"🇫🇷",
    "Bottas":"🇫🇮","Zhou":"🇨🇳","Albon":"🇹🇭",
    "Hulkenberg":"🇩🇪","Magnussen":"🇩🇰","Tsunoda":"🇯🇵",
    "Lawson":"🇳🇿","Bearman":"🇬🇧","Colapinto":"🇦🇷","Doohan":"🇦🇺",
    "Antonelli":"🇮🇹","Hadjar":"🇫🇷","Bortoleto":"🇧🇷",
}
def drv_flag(last_name: str) -> str:
    return _DRIVER_FLAG.get(last_name, "🏁")

# ─── API helpers ──────────────────────────────────────────────────────────────
http: aiohttp.ClientSession = None

async def oget(path) -> list:
    try:
        async with http.get(f"{O}{path}", timeout=TIMEOUT) as r:
            if r.status == 200:
                d = await r.json(content_type=None)
                return d if isinstance(d, list) else []
    except Exception as e:
        print(f"[API ERR] {path}: {e}")
    return []

async def list_recent_sessions(n=10):
    sessions = await oget(f"/sessions?year=2025")
    sessions += await oget(f"/sessions?year=2026")
    races = [s for s in sessions if s.get("session_type") in ("Race","Qualifying","Sprint")]
    races.sort(key=lambda s: s.get("date_start",""), reverse=True)
    print(f"\n{'SK':>6}  {'Дата':12}  {'Тип':14}  {'Гонка'}")
    print("─"*60)
    for s in races[:n]:
        sk   = s["session_key"]
        date = s.get("date_start","")[:10]
        stype= s.get("session_name","?")
        gp   = s.get("meeting_name","?")
        print(f"{sk:>6}  {date:12}  {stype:14}  {gp}")
    print()

async def get_session_info(sk: int) -> dict:
    d = await oget(f"/sessions?session_key={sk}")
    return d[0] if d else {}

async def get_drivers(sk: int) -> dict:
    d = await oget(f"/drivers?session_key={sk}")
    return {x["driver_number"]: x for x in d}

async def get_rc(sk: int) -> list:
    return await oget(f"/race_control?session_key={sk}")

async def get_pits(sk: int) -> list:
    return await oget(f"/pit?session_key={sk}")

async def get_laps(sk: int) -> list:
    laps = await oget(f"/laps?session_key={sk}")
    return [l for l in laps if l.get("lap_duration") and 60 < l["lap_duration"] < 200]

async def get_positions(sk: int) -> list:
    return await oget(f"/position?session_key={sk}")

def parse_dt(s: str) -> datetime:
    """ISO → datetime UTC."""
    if not s:
        return datetime.min.replace(tzinfo=timezone.utc)
    s = s.rstrip("Z")
    if "+" not in s:
        s += "+00:00"
    try:
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)

# ─── Simulator ────────────────────────────────────────────────────────────────
class LiveSimulator:
    """
    Воспроизводит исторические данные гонки поэтапно в хронологическом порядке.
    Все события группируются по времени и проигрываются последовательно.
    """

    def __init__(self, sk: int, sess_info: dict, drivers: dict,
                 speed: int = 30,
                 telegram_token: str = None, telegram_chat: str = None):
        self.sk       = sk
        self.info     = sess_info
        self.drivers  = drivers
        self.speed    = speed   # ×N от реального времени
        self.gp       = sess_info.get("meeting_name", f"Session {sk}")
        self.stype    = sess_info.get("session_name", "Race").lower()
        self.is_race  = "race" in self.stype

        self.telegram_token = telegram_token
        self.telegram_chat  = telegram_chat

        # Статистика
        self.stats = {
            "messages": 0, "flags": 0, "pits": 0,
            "best_laps": 0, "podiums": 0
        }

        # Дедупликация
        self.seen_rc:  set = set()
        self.seen_pit: set = set()
        self.best_lap: float = None
        self.form_sent  = False
        self.start_sent = False
        self.finish_sent= False
        self.podium_sent: set = set()

    def _name(self, n) -> str:
        d = self.drivers.get(n, {})
        return d.get("full_name") or d.get("broadcast_name") or f"#{n}"

    def _team(self, n) -> str:
        return self.drivers.get(n, {}).get("team_name") or ""

    def _flag(self, n) -> str:
        full = self._name(n)
        last = full.split()[-1] if full else ""
        return drv_flag(last)

    async def send(self, text: str, event_type: str = ""):
        """Выводит в консоль и (опционально) шлёт в Telegram."""
        self.stats["messages"] += 1
        clean = strip_html(text)
        ts = datetime.now().strftime("%H:%M:%S")
        border = "═" * 50
        print(f"\n{border}")
        print(f"  [{ts}] {event_type}")
        print(border)
        print(clean)

        if self.telegram_token and self.telegram_chat:
            try:
                url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
                async with http.post(url, json={
                    "chat_id": self.telegram_chat,
                    "text": text,
                    "parse_mode": "HTML"
                }, timeout=TIMEOUT) as r:
                    if r.status != 200:
                        body = await r.text()
                        print(f"  ⚠️  Telegram error: {body[:100]}")
            except Exception as e:
                print(f"  ⚠️  Telegram send failed: {e}")

    # ── RC events ─────────────────────────────────────────────────────────────
    async def handle_rc(self, m: dict):
        text = m.get("message", "")
        flag = (m.get("flag") or "").upper()
        up   = text.upper()

        if ("FORMATION LAP" in up or "FORMATION" in up) and not self.form_sent:
            self.form_sent = True
            label = "Прогревочный круг" if self.is_race else "Сессия начинается"
            await self.send(f"🏎️ <b>{label}!</b>\n\n<b>{self.gp}</b>", "🏎️ ФОРМЕЙШН ЛАП")
            self.stats["flags"] += 1

        elif (flag == "GREEN" or "GREEN LIGHT" in up or "RACE START" in up) \
                and not self.start_sent and self.is_race:
            self.start_sent = True
            await self.send(f"🚦 <b>ГОНКА СТАРТОВАЛА!</b>\n\n<b>{self.gp}</b>", "🚦 СТАРТ")

        elif "SAFETY CAR" in up and "VIRTUAL" not in up and "MEDICAL" not in up:
            if "DEPLOYED" in up or flag == "SC":
                await self.send(f"🚗 <b>Safety Car!</b>\n{text}", "🚗 SC DEPLOYED")
                self.stats["flags"] += 1
            elif "WITHDRAWN" in up or "IN THIS LAP" in up:
                await self.send("🚗 <b>Safety Car возвращается в боксы</b>", "🚗 SC IN")

        elif flag == "VSC" or "VIRTUAL SAFETY CAR" in up:
            if "DEPLOYED" in up or flag == "VSC":
                await self.send("🔶 <b>Virtual Safety Car (VSC)!</b>", "🔶 VSC")
                self.stats["flags"] += 1
            elif "ENDING" in up or "RESUMED" in up:
                await self.send("🔶 <b>VSC заканчивается — рестарт!</b>", "🔶 VSC ENDING")

        elif flag == "YELLOW":
            scope = m.get("scope", "")
            await self.send(
                f"🟡 <b>Жёлтый флаг</b>{' · Сектор ' + scope if scope else ''}\n{text}",
                "🟡 YELLOW"
            )
            self.stats["flags"] += 1

        elif flag == "RED":
            await self.send(f"🔴 <b>КРАСНЫЙ ФЛАГ!</b>\n\n{self.gp}\n{text}", "🔴 RED FLAG")
            self.stats["flags"] += 1

        elif "PENALTY" in up or "SANCTION" in up or "DRIVE THROUGH" in up or "STOP AND GO" in up:
            await self.send(f"⚖️ <b>Штраф</b>\n{text}", "⚖️ PENALTY")

        elif (flag == "CHEQUERED" or "CHEQUERED" in up or "CHECKERED" in up) \
                and not self.finish_sent:
            self.finish_sent = True
            label = "Гонка" if self.is_race else "Сессия"
            await self.send(f"🏁 <b>{label} завершена!</b>\n\n<b>{self.gp}</b>", "🏁 ФИНИШ")

    # ── Pitstop ───────────────────────────────────────────────────────────────
    async def handle_pit(self, p: dict):
        dur = p.get("pit_duration")
        if not dur:
            return
        pid = f"{p.get('driver_number')}_{p.get('lap_number')}_{dur}"
        if pid in self.seen_pit:
            return
        self.seen_pit.add(pid)
        dn   = p.get("driver_number")
        name = self._name(dn); flag = self._flag(dn); team = self._team(dn)
        self.stats["pits"] += 1
        await self.send(
            f"🔧 <b>Пит-стоп!</b>\n"
            f"{flag} <b>{name}</b> ({team})\n"
            f"📌 Круг {p.get('lap_number','?')}  ·  ⏱ {float(dur):.1f} с",
            "🔧 PIT"
        )

    # ── Fast lap ──────────────────────────────────────────────────────────────
    async def handle_lap(self, l: dict):
        dur = l.get("lap_duration")
        if not dur or not (60 < dur < 200):
            return
        if self.best_lap is not None and dur >= self.best_lap:
            return
        self.best_lap = dur
        dn   = l.get("driver_number")
        mins = int(dur // 60); secs = dur % 60
        name = self._name(dn); flag = self._flag(dn); team = self._team(dn)
        self.stats["best_laps"] += 1
        await self.send(
            f"⚡ <b>Новый быстрый круг!</b>\n"
            f"{flag} <b>{name}</b> ({team})\n"
            f"⏱ <b>{mins}:{secs:06.3f}</b>  ·  Круг {l.get('lap_number','?')}",
            "⚡ FASTEST LAP"
        )

    # ── Podium ────────────────────────────────────────────────────────────────
    async def handle_positions(self, positions_snapshot: dict, lap: int):
        if len(self.podium_sent) >= 3:
            return
        medals = {1: "🥇", 2: "🥈", 3: "🥉"}
        for dn, p in positions_snapshot.items():
            pos = p.get("position")
            if pos in (1, 2, 3) and f"P{pos}" not in self.podium_sent:
                if lap >= 45:
                    self.podium_sent.add(f"P{pos}")
                    name = self._name(dn); flag = self._flag(dn); team = self._team(dn)
                    self.stats["podiums"] += 1
                    await self.send(
                        f"{medals[pos]} <b>P{pos} — {name}!</b>\n"
                        f"{flag} ({team})  ·  Круг {lap}",
                        f"{medals[pos]} PODIUM P{pos}"
                    )

    # ── Main simulation ───────────────────────────────────────────────────────
    async def run(self, rc_events: list, pits: list, laps: list, positions: list):
        print(f"\n{'█'*50}")
        print(f"  🏎️  СИМУЛЯЦИЯ ГОНКИ: {self.gp}")
        print(f"  📡  Session key: {self.sk}")
        print(f"  ⚡  Скорость: ×{self.speed}")
        print(f"  📊  Данные: {len(rc_events)} RC | {len(pits)} питов | {len(laps)} кругов")
        print(f"{'█'*50}\n")

        if not rc_events:
            print("❌ Race Control данные пусты — возможно сессия ещё не прошла")
            return

        # Строим единую хронологическую очередь событий
        events = []  # (datetime, type, data)

        for m in rc_events:
            dt = parse_dt(m.get("date", ""))
            events.append((dt, "rc", m))

        for p in pits:
            dt = parse_dt(p.get("date", ""))
            events.append((dt, "pit", p))

        for l in laps:
            dt = parse_dt(l.get("date_start", ""))
            events.append((dt, "lap", l))

        # Позиции группируем по кругам
        pos_by_lap: dict = {}
        for p in positions:
            lap = p.get("lap_number", 0) or 0
            dn  = p.get("driver_number")
            if lap not in pos_by_lap:
                pos_by_lap[lap] = {}
            pos_by_lap[lap][dn] = p

        events.sort(key=lambda e: e[0])

        if not events:
            print("❌ Нет событий для воспроизведения")
            return

        t_start = events[0][0]
        t_end   = events[-1][0]
        duration = (t_end - t_start).total_seconds()
        print(f"  ⏱  Длительность сессии: {int(duration//3600)}ч {int((duration%3600)//60)}м")
        print(f"  ▶️  Старт воспроизведения...\n")

        prev_real = t_start
        sim_delay_total = 0.0
        max_delay = 3.0  # не спать дольше 3 сек между событиями

        for i, (evt_dt, etype, data) in enumerate(events):
            # Задержка пропорциональна разнице времён (ускоренная)
            gap = (evt_dt - prev_real).total_seconds()
            delay = min(gap / self.speed, max_delay)
            if delay > 0.05:
                await asyncio.sleep(delay)
                sim_delay_total += delay
            prev_real = evt_dt

            if etype == "rc":
                uid = f"{data.get('date','')}{data.get('message','')}"
                if uid not in self.seen_rc:
                    self.seen_rc.add(uid)
                    await self.handle_rc(data)

            elif etype == "pit":
                await self.handle_pit(data)

            elif etype == "lap":
                await self.handle_lap(data)
                # После каждого круга проверяем позиции
                lap_n = data.get("lap_number", 0)
                if lap_n in pos_by_lap:
                    await self.handle_positions(pos_by_lap[lap_n], lap_n)

        # Итоги
        print(f"\n{'═'*50}")
        print(f"  ✅  СИМУЛЯЦИЯ ЗАВЕРШЕНА")
        print(f"{'═'*50}")
        print(f"  📨  Сообщений отправлено: {self.stats['messages']}")
        print(f"  🚩  Флагов/событий:       {self.stats['flags']}")
        print(f"  🔧  Пит-стопов:           {self.stats['pits']}")
        print(f"  ⚡  Быстрых кругов:       {self.stats['best_laps']}")
        print(f"  🏆  Подиум-сообщений:     {self.stats['podiums']}")
        real_min = sim_delay_total / 60
        print(f"  ⏱  Реальное время теста: {real_min:.1f} мин")
        print(f"{'═'*50}\n")

# ─── Entry point ──────────────────────────────────────────────────────────────
async def main():
    parser = argparse.ArgumentParser(description="F1 Live Monitor Test")
    parser.add_argument("--sk",      type=int, default=None, help="OpenF1 session_key")
    parser.add_argument("--speed",   type=int, default=30,   help="Скорость ×N (по умолч. 30)")
    parser.add_argument("--token",   type=str, default=None, help="Telegram bot token")
    parser.add_argument("--chat",    type=str, default=None, help="Telegram chat_id")
    parser.add_argument("--list",    action="store_true",    help="Список последних сессий")
    args = parser.parse_args()

    global http
    http = aiohttp.ClientSession()

    try:
        if args.list:
            await list_recent_sessions(15)
            return

        # Определяем session_key
        if args.sk:
            sk = args.sk
        else:
            print("🔍 Ищем последнюю гонку в OpenF1...")
            races_25 = await oget("/sessions?session_type=Race&year=2025")
            races_26 = await oget("/sessions?session_type=Race&year=2026")
            all_races = races_25 + races_26
            all_races.sort(key=lambda s: s.get("date_start",""), reverse=True)
            if not all_races:
                print("❌ Не удалось найти сессии. Укажи --sk вручную.")
                return
            sk = all_races[0]["session_key"]
            print(f"   → Выбрана: {all_races[0].get('meeting_name')} {all_races[0].get('session_name')} (sk={sk})")

        print(f"\n📡 Загружаем данные для session_key={sk}...")
        sess_info, drivers, rc, pits, laps, pos = await asyncio.gather(
            get_session_info(sk),
            get_drivers(sk),
            get_rc(sk),
            get_pits(sk),
            get_laps(sk),
            get_positions(sk),
        )

        if not sess_info:
            print(f"❌ Session {sk} не найдена в OpenF1")
            return

        print(f"   ✅ {sess_info.get('meeting_name')} — {sess_info.get('session_name')}")
        print(f"   Гонщиков: {len(drivers)} | RC: {len(rc)} | Питов: {len(pits)} | Кругов: {len(laps)} | Позиций: {len(pos)}")

        token = args.token
        chat  = args.chat

        if token and chat:
            print(f"\n📲 Telegram режим: сообщения будут слаться в чат {chat}")
        else:
            print("\n🖥️  Консольный режим (без --token и --chat)")

        sim = LiveSimulator(
            sk=sk,
            sess_info=sess_info,
            drivers=drivers,
            speed=args.speed,
            telegram_token=token,
            telegram_chat=chat,
        )
        await sim.run(rc, pits, laps, pos)

    finally:
        await http.close()

if __name__ == "__main__":
    asyncio.run(main())
