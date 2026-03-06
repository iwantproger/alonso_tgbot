from __future__ import annotations
import asyncio, logging
from datetime import datetime, timedelta
import pytz
import api, storage
from formatters import fmt_pit_stop, fmt_race_control, fmt_fastest_lap, fmt_finish, fmt_leclerc_stats, fmt_qualifying_result, fmt_race_result
from schedule_parser import get_all_sessions
from constants import tire

log = logging.getLogger(__name__)
POLL_INTERVAL = 15

class LiveMonitor:
    def __init__(self):
        self._app = None
        self.session_key = None
        self.session_type = None
        self.seen_rc = set()
        self.seen_pits = set()
        self.current_positions = {}
        self.current_fastest = None
        self.drivers = {}
        self.stints = {}
        self.formation_sent = False
        self.race_start_sent = False
        self.race_end_sent = False
        self.podium_sent = set()
        self.post_quali_sent = False
        self.post_race_sent = False

    def start(self, app):
        self._app = app
        asyncio.create_task(self._loop())
        log.info("LiveMonitor started")

    async def _loop(self):
        while True:
            try: await self._tick()
            except Exception as e: log.exception("LiveMonitor: %s", e)
            await asyncio.sleep(POLL_INTERVAL)

    def _get_active_ics_session(self, now):
        for s in get_all_sessions():
            if s.start_utc <= now <= s.start_utc + timedelta(hours=4):
                return s
        return None

    async def _tick(self):
        now = datetime.now(tz=pytz.utc)
        active = self._get_active_ics_session(now)
        if not active:
            if self.session_key:
                await self._on_session_end()
            return
        of1s = await api.get_latest_session()
        if not of1s: return
        sk   = of1s.get("session_key")
        stype = of1s.get("session_type","")
        if sk != self.session_key:
            self.session_key = sk; self.session_type = stype
            self.seen_rc = set(); self.seen_pits = set()
            self.current_positions = {}; self.current_fastest = None
            self.stints = {}; self.formation_sent = False
            self.race_start_sent = False; self.race_end_sent = False
            self.podium_sent = set(); self.post_quali_sent = False; self.post_race_sent = False
            log.info("New session: %s (%s)", sk, stype)
        self.drivers = await api.get_drivers(sk) or self.drivers
        await self._check_race_control(sk)
        await self._check_pit_stops(sk)
        if "Race" in stype:
            await self._check_positions(sk)
            await self._check_fastest_lap(sk)

    async def _check_race_control(self, sk):
        messages = await api.get_race_control(sk)
        for msg in messages:
            uid = f"{msg.get('date','')}_{msg.get('message','')}"
            if uid in self.seen_rc: continue
            self.seen_rc.add(uid)
            flag = msg.get("flag",""); lap = msg.get("lap_number") or 0
            if self.session_type == "Race":
                if flag == "GREEN" and not self.formation_sent and int(lap) == 0:
                    self.formation_sent = True
                    await self._send("🟢 <b>Прогревочный круг!</b>\nМашины выехали на трассу! Старт гонки скоро!")
                    continue
                if flag == "GREEN" and not self.race_start_sent and int(lap) >= 1:
                    self.race_start_sent = True
                    await self._send("🏎️ <b>СТАРТ ГОНКИ!</b>\nОфициальный старт — гонка пошла!")
                    continue
            text = fmt_race_control(msg)
            if text: await self._send(text)

    async def _check_pit_stops(self, sk):
        pits   = await api.get_pit_stops(sk)
        stints = await api.get_stints(sk)
        for st in stints:
            dn = st.get("driver_number")
            if dn is None: continue
            self.stints.setdefault(dn, [])
            if not any(s.get("stint_number")==st.get("stint_number") for s in self.stints[dn]):
                self.stints[dn].append(st)
        for pit in pits:
            dn = pit.get("driver_number"); lap = pit.get("lap_number")
            uid = f"{dn}_{lap}"
            if uid in self.seen_pits: continue
            self.seen_pits.add(uid)
            drv  = self.drivers.get(dn, {})
            name = drv.get("full_name") or drv.get("name_acronym") or f"#{dn}"
            team = drv.get("team_name","")
            dn_stints = sorted(self.stints.get(dn, []), key=lambda s: s.get("stint_number",0))
            prev_c = dn_stints[-2].get("compound") if len(dn_stints)>=2 else None
            new_c  = dn_stints[-1].get("compound") if len(dn_stints)>=1 else None
            await self._send(fmt_pit_stop(pit, prev_c, new_c, name, team))

    async def _check_positions(self, sk):
        positions = await api.get_positions(sk)
        latest = {}
        for p in positions:
            dn = p.get("driver_number")
            if dn: latest[dn] = p
        self.current_positions = {dn: d.get("position",99) for dn,d in latest.items()}
        for dn, pos in self.current_positions.items():
            if pos in (1,2,3) and pos not in self.podium_sent:
                drv = self.drivers.get(dn, {})
                name = drv.get("full_name") or f"#{dn}"
                team = drv.get("team_name","")
                self.podium_sent.add(pos)
                await self._send(fmt_finish(pos, name, team, "—"))

    async def _check_fastest_lap(self, sk):
        laps = await api.of1("laps", session_key=sk) or []
        valid = [l for l in laps if l.get("lap_duration") and not l.get("is_pit_out_lap")]
        if not valid: return
        best = min(valid, key=lambda l: l["lap_duration"])
        prev = (self.current_fastest or {}).get("lap_duration", float("inf"))
        if best["lap_duration"] < prev:
            self.current_fastest = best
            dn = best.get("driver_number"); drv = self.drivers.get(dn,{})
            name = drv.get("full_name") or f"#{dn}"; team = drv.get("team_name","")
            pos = self.current_positions.get(dn)
            t = best["lap_duration"]; mins = int(t//60); secs = t%60
            t_s = f"{mins}:{secs:06.3f}"
            await self._send(fmt_fastest_lap(t_s, name, team, pos))

    async def _on_session_end(self):
        if not self.session_key: return
        stype = self.session_type or ""
        self.session_key = None
        if "Qualifying" in stype and not self.post_quali_sent:
            self.post_quali_sent = True
            results = await api.get_last_qualifying()
            if results: await self._send(fmt_qualifying_result(results))
        elif "Race" in stype and not self.post_race_sent:
            self.post_race_sent = True
            race, results = await api.get_last_race_results()
            standings = await api.get_driver_standings()
            if race and results: await self._send(fmt_race_result(race, results, standings))
            quali = await api.get_last_qualifying()
            await self._send(fmt_leclerc_stats(quali, results))

    async def _send(self, text):
        if not self._app: return
        subs = storage.all_subscribers()
        for cid in subs:
            try: await self._app.bot.send_message(cid, text, parse_mode="HTML")
            except Exception as e: log.warning("Send to %s: %s", cid, e)
        log.info("Live → %d: %.60s", len(subs), text)

monitor = LiveMonitor()
