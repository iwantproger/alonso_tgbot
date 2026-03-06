#!/usr/bin/env python3
import asyncio, json, logging, os, re
from collections import OrderedDict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import aiohttp, pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from icalendar import Calendar
from telegram import (CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
                      ReplyKeyboardMarkup, Update)
from telegram.ext import (Application, CallbackQueryHandler, CommandHandler,
                           ContextTypes, MessageHandler, filters)

BOT_TOKEN = os.environ["BOT_TOKEN"]
STORAGE   = Path(__file__).parent / "data" / "subscribers.json"
ICS_FILE  = Path(__file__).parent / "f1_2026.ics"
MSK       = pytz.timezone("Europe/Moscow")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
STORAGE.parent.mkdir(exist_ok=True)
http: Optional[aiohttp.ClientSession] = None
_app: Optional[Application] = None
scheduler = AsyncIOScheduler(timezone=pytz.utc)

# ── STORAGE ──────────────────────────────────────────────────────────────────
def _load():
    return json.loads(STORAGE.read_text("utf-8")) if STORAGE.exists() else {"users":[],"chats":[],"muted":[]}
def _save(d): STORAGE.write_text(json.dumps(d, ensure_ascii=False, indent=2), "utf-8")
def is_subscribed(cid): d=_load(); return cid in d["users"] or cid in d["chats"]
def is_muted(cid): return cid in _load().get("muted",[])
def toggle_notif(cid, priv):
    d=_load(); key="users" if priv else "chats"
    if cid not in d[key]: d[key].append(cid)
    muted=d.setdefault("muted",[])
    if cid in muted: muted.remove(cid); _save(d); return True
    muted.append(cid); _save(d); return False
def subscribe(cid, priv):
    d=_load(); key="users" if priv else "chats"
    if cid not in d[key]: d[key].append(cid); _save(d); return True
    return False
def active_subs():
    d=_load(); muted=set(d.get("muted",[])); return [c for c in d["users"]+d["chats"] if c not in muted]

# ── FLAGS ─────────────────────────────────────────────────────────────────────
_FL = {"саудовской аравии":"🇸🇦","великобритании":"🇬🇧","барселоны-каталонии":"🇪🇸",
       "сан-паулу":"🇧🇷","лас-вегаса":"🇺🇸","абу-даби":"🇦🇪","австралии":"🇦🇺",
       "австрии":"🇦🇹","азербайджана":"🇦🇿","барселоны":"🇪🇸","испании":"🇪🇸",
       "бахрейна":"🇧🇭","бельгии":"🇧🇪","венгрии":"🇭🇺","италии":"🇮🇹",
       "канады":"🇨🇦","катара":"🇶🇦","китая":"🇨🇳","майами":"🇺🇸","мехико":"🇲🇽",
       "монако":"🇲🇨","нидерландов":"🇳🇱","сша":"🇺🇸","сингапура":"🇸🇬","японии":"🇯🇵"}
def flag(name):
    low=name.lower()
    for k in sorted(_FL,key=len,reverse=True):
        if k in low: return _FL[k]
    return "🏁"

# ── LOCALE ────────────────────────────────────────────────────────────────────
MR={1:"Январь",2:"Февраль",3:"Март",4:"Апрель",5:"Май",6:"Июнь",
    7:"Июль",8:"Август",9:"Сентябрь",10:"Октябрь",11:"Ноябрь",12:"Декабрь"}
MG={1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
    7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}

# ── ICS PARSING ───────────────────────────────────────────────────────────────
SM={"1-я сессия свободных заездов":("🔧","П1"),"2-я сессия свободных заездов":("🔧","П2"),
    "3-я сессия свободных заездов":("🔧","П3"),"квалификация к спринту":("⚡","Кв.Спринта"),
    "спринт":("⚡","Спринт"),"квалификация":("🔥","Квалификация"),"гонка":("🏁","Гонка")}
def sess_meta(s):
    low=s.lower()
    for k,v in SM.items():
        if k in low: return v
    return "🏎️",s
def gp_base(s): return s.split(". ")[0] if ". " in s else s

def parse_ics():
    evs=[]
    with open(ICS_FILE,"rb") as f: cal=Calendar.from_ical(f.read())
    for c in cal.walk():
        if c.name!="VEVENT": continue
        summ=str(c.get("SUMMARY",""))
        loc=str(c.get("LOCATION","")).replace("\\,",",")
        st=c.get("DTSTART").dt
        if not isinstance(st,datetime): st=datetime(st.year,st.month,st.day,tzinfo=pytz.utc)
        elif st.tzinfo is None: st=pytz.utc.localize(st)
        evs.append({"summary":summ,"location":loc,"start_utc":st})
    evs.sort(key=lambda e:e["start_utc"]); return evs

def build_weekends():
    evs=parse_ics(); wm=OrderedDict()
    for ev in evs:
        base=gp_base(ev["summary"])
        if base not in wm:
            parts=ev["location"].split(","); city=parts[0].strip()
            country=parts[-1].strip() if len(parts)>1 else city
            wm[base]={"gp_name":base,"country":country,"city":city,"flag":flag(base),
                      "location":ev["location"],"sessions":[],"id":re.sub(r"[^a-z0-9]","_",base.lower())[:28]}
        e,s=sess_meta(ev["summary"])
        wm[base]["sessions"].append({"summary":ev["summary"],"short":s,"emoji":e,
                                      "start_utc":ev["start_utc"],"location":ev["location"]})
    wknds=list(wm.values())
    for w in wknds: w["start_utc"]=w["sessions"][0]["start_utc"]; w["end_utc"]=w["sessions"][-1]["start_utc"]
    wknds.sort(key=lambda w:w["start_utc"]); return wknds

def cur_weekend(wks):
    now=datetime.now(tz=pytz.utc)
    for w in wks:
        if w["start_utc"]-timedelta(hours=6)<=now<=w["end_utc"]+timedelta(hours=4): return w
    return None
def nxt_weekend(wks):
    now=datetime.now(tz=pytz.utc)
    for w in wks:
        if w["start_utc"]>now: return w
    return None
def nxt_session(wks):
    now=datetime.now(tz=pytz.utc)
    for w in wks:
        for s in w["sessions"]:
            if s["start_utc"]>now:
                return {**s,"gp_name":w["gp_name"],"flag":w["flag"],"country":w["country"],"city":w["city"]}
    return None

# ── WEATHER ───────────────────────────────────────────────────────────────────
# Русские названия городов → английские для wttr.in
_CITY_EN = {
    "мельбурн":"Melbourne","шанхай":"Shanghai","сузука":"Suzuka",
    "сахир":"Sakhir","джидда":"Jeddah","майами":"Miami",
    "монте-карло":"Monte Carlo","монако":"Monaco","монреаль":"Montreal",
    "барселона":"Barcelona","шпильберг":"Spielberg","сильверстоун":"Silverstone",
    "будапешт":"Budapest","спа":"Spa","зандворт":"Zandvoort","монца":"Monza",
    "баку":"Baku","сингапур":"Singapore","остин":"Austin",
    "мехико":"Mexico City","сан-паулу":"Sao Paulo","лас-вегас":"Las Vegas",
    "лусаил":"Lusail","абу-даби":"Abu Dhabi",
}
def _city_en(city):
    low=city.lower()
    for k,v in _CITY_EN.items():
        if k in low: return v
    return city

_WX={"Sunny":"☀️ Солнечно","Clear":"☀️ Ясно","Partly cloudy":"⛅ Переменная облачность",
     "Cloudy":"☁️ Облачно","Overcast":"☁️ Пасмурно","Mist":"🌫 Туман","Fog":"🌫 Туман",
     "Light rain":"🌦 Лёгкий дождь","Moderate rain":"🌧 Дождь","Heavy rain":"🌧 Сильный дождь",
     "Patchy rain possible":"🌦 Возможен дождь","Light drizzle":"🌦 Морось",
     "Thundery outbreaks":"⛈ Гроза","Light snow":"🌨 Лёгкий снег","Heavy snow":"🌨 Снег"}
def _wx(en):
    for k,v in _WX.items():
        if k.lower() in en.lower(): return v
    return en

async def get_weather(city):
    city_q=_city_en(city)
    try:
        url=f"https://wttr.in/{city_q}?format=j1"
        async with http.get(url,timeout=aiohttp.ClientTimeout(total=8)) as r:
            data=await r.json(content_type=None)
        cur=data["current_condition"][0]
        hourly=data["weather"][0]["hourly"]
        rain=max(int(h.get("chanceofrain",0)) for h in hourly[:4])
        desc=_wx(cur["weatherDesc"][0]["value"])
        return {"temp":cur["temp_C"],"feels":cur["FeelsLikeC"],"desc":desc,
                "hum":cur["humidity"],"wind":cur["windspeedKmph"],"rain":rain}
    except Exception as e:
        log.warning("Погода '%s'→'%s': %s",city,city_q,e); return {}

def fmt_weather(w):
    if not w: return "🌡 Погода временно недоступна"
    rain=f"☔ {w['rain']}%" if int(w["rain"])>5 else "☀️ Без осадков"
    return f"🌡 <b>{w['temp']}°C</b> (ощущается {w['feels']}°C) · {w['desc']}\n💧 {w['hum']}% · 💨 {w['wind']} км/ч · {rain}"

# ── JOLPICA API ───────────────────────────────────────────────────────────────
J="https://api.jolpi.ca/ergast/f1"
async def jget(path):
    try:
        async with http.get(f"{J}{path}",timeout=aiohttp.ClientTimeout(total=8)) as r:
            return await r.json(content_type=None)
    except: return None

async def driver_standings():
    d=await jget("/current/driverStandings.json")
    if not d: return []
    lst=d["MRData"]["StandingsTable"]["StandingsLists"]
    return lst[0]["DriverStandings"] if lst else []

async def last_quali():
    d=await jget("/current/last/qualifying.json")
    if not d: return "",[]
    races=d["MRData"]["RaceTable"]["Races"]
    if not races: return "",[]
    return races[0].get("raceName",""),races[0].get("QualifyingResults",[])

async def last_race():
    d=await jget("/current/last/results.json")
    if not d: return "",[]
    races=d["MRData"]["RaceTable"]["Races"]
    if not races: return "",[]
    return races[0].get("raceName",""),races[0].get("Results",[])

# ── OPENF1 API ────────────────────────────────────────────────────────────────
O="https://api.openf1.org/v1"
async def oget(path):
    try:
        async with http.get(f"{O}{path}",timeout=aiohttp.ClientTimeout(total=6)) as r:
            return await r.json(content_type=None)
    except: return []

async def f1_latest_sess():
    d=await oget("/sessions?session_key=latest"); return d[0] if d else None
async def f1_drivers(sk): d=await oget(f"/drivers?session_key={sk}"); return {x["driver_number"]:x for x in d}
async def f1_rc(sk): return await oget(f"/race_control?session_key={sk}")
async def f1_pit(sk): return await oget(f"/pit?session_key={sk}")
async def f1_laps(sk): return await oget(f"/laps?session_key={sk}")

# ── FORMATTERS ────────────────────────────────────────────────────────────────
def _msk(dt): return dt.astimezone(MSK)
def dtstr(dt): m=_msk(dt); return f"{m.day} {MG[m.month]}, {m.strftime('%H:%M')} МСК"

def fmt_card(w):
    s=_msk(w["start_utc"]); e=_msk(w["end_utc"])
    dates=(f"{s.day}–{e.day} {MG[s.month]}" if s.month==e.month
           else f"{s.day} {MG[s.month]} – {e.day} {MG[e.month]}")
    lines=[
        f"",
        f"{w['flag']}  <b>{w['gp_name'].upper()}</b>",
        f"📍 {w['country']}, {w['city']}   ·   📅 {dates}",
        f"<code>{'─'*32}</code>",
    ]
    for ss in w["sessions"]:
        dt=_msk(ss["start_utc"])
        lines.append(f"  {ss['emoji']}  {ss['short']:<16}{dt.strftime('%d.%m  %H:%M')} МСК")
    return "\n".join(lines)

def fmt_month(wks,year,month):
    mwks=[w for w in wks if _msk(w["start_utc"]).year==year and _msk(w["start_utc"]).month==month]
    hdr=f"🗓 <b>{MR[month]} {year}</b>  —  {len(mwks)} уикенда"
    sep="\n\n<b>┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄</b>\n"
    cards=sep.join(fmt_card(w) for w in mwks)
    return (hdr+"\n"+cards)[:4090]

async def fmt_cur_weekend(w):
    lines=["🏁 <b>Текущий / ближайший гоночный уикенд</b>\n",fmt_card(w)]
    stds=await driver_standings()
    if stds:
        lines.append("\n\n🏆 <b>Чемпионат гонщиков 2026</b>")
        for s in stds[:10]:
            d=s["Driver"]; name=f"{d['givenName'][0]}. {d['familyName']}"
            team=(s.get("Constructors") or [{}])[0].get("name","—")
            lines.append(f"  {s['position']}. <b>{name}</b> ({team}) · {s['points']} очк.")
    return "\n".join(lines)

TZ_MAP={"мельбурн":"Australia/Melbourne","шанхай":"Asia/Shanghai","сузука":"Asia/Tokyo",
        "сахир":"Asia/Bahrain","джидда":"Asia/Riyadh","майами":"America/New_York",
        "монте-карло":"Europe/Monaco","монако":"Europe/Monaco","монреаль":"America/Toronto",
        "барселона":"Europe/Madrid","шпильберг":"Europe/Vienna","сильверстоун":"Europe/London",
        "будапешт":"Europe/Budapest","спа":"Europe/Brussels","зандворт":"Europe/Amsterdam",
        "монца":"Europe/Rome","баку":"Asia/Baku","сингапур":"Asia/Singapore",
        "остин":"America/Chicago","мехико":"America/Mexico_City","сан-паулу":"America/Sao_Paulo",
        "лас-вегас":"America/Los_Angeles","лусаил":"Asia/Qatar","абу-даби":"Asia/Dubai"}
def local_tz(city):
    low=city.lower()
    for k,tz in TZ_MAP.items():
        if k in low: return pytz.timezone(tz)
    return MSK

async def fmt_next_sess(sess):
    city=sess["city"]; msk_dt=_msk(sess["start_utc"])
    loc=sess["start_utc"].astimezone(local_tz(city))
    lines=[f"{sess['flag']} <b>Следующее событие</b>\n",
           f"{sess['emoji']} <b>{sess['summary']}</b>",
           f"📍 {sess['country']}, {city}",
           f"🕐 <b>{msk_dt.strftime('%H:%M')} МСК</b>  ·  {loc.strftime('%H:%M')} местного",
           f"📅 {msk_dt.day} {MG[msk_dt.month]}",""]
    w=await get_weather(city); lines.append(fmt_weather(w))
    if "гонка" in sess["summary"].lower():
        _,qr=await last_quali()
        if qr:
            lines.append("\n🏁 <b>Стартовая решётка (топ-10):</b>")
            for q in qr[:10]:
                d=q["Driver"]; best=q.get("Q3") or q.get("Q2") or q.get("Q1") or "—"
                lines.append(f"  {q['position']}. <b>{d['givenName'][0]}. {d['familyName']}</b> ({q['Constructor']['name']})  {best}")
    return "\n".join(lines)

async def fmt_standings():
    stds=await driver_standings()
    if not stds: return "❌ Данные чемпионата ещё недоступны — сезон не начался"
    lines=["🏆 <b>Чемпионат гонщиков 2026</b>\n"]
    for s in stds[:20]:
        d=s["Driver"]; team=(s.get("Constructors") or [{}])[0].get("name","—")
        lines.append(f"{int(s['position']):>2}. <b>{d['givenName'][0]}. {d['familyName']}</b> ({team}) — {s['points']} очк.")
    return "\n".join(lines)

async def fmt_quali():
    rname,results=await last_quali()
    if not results: return "❌ Результаты квалификации недоступны — сезон ещё не начался"
    lines=[f"🔥 <b>Квалификация — {rname}</b>\n"]
    q3=[r for r in results if r.get("Q3")]
    q2=[r for r in results if r.get("Q2") and not r.get("Q3")]
    q1=[r for r in results if not r.get("Q2")]
    if q3:
        lines.append("✅ <b>Q3 — проходят в топ-10:</b>")
        for r in q3:
            d=r["Driver"]
            lines.append(f"  {r['position']}. <b>{d['givenName'][0]}. {d['familyName']}</b> ({r['Constructor']['name']})  {r['Q3']}")
    if q2:
        lines.append("\n⚠️ <b>Выбыли в Q2:</b>")
        for r in q2: d=r["Driver"]; lines.append(f"  {r['position']}. {d['givenName'][0]}. {d['familyName']}  {r['Q2']}")
    if q1:
        lines.append("\n❌ <b>Выбыли в Q1:</b>")
        for r in q1: d=r["Driver"]; lines.append(f"  {r['position']}. {d['givenName'][0]}. {d['familyName']}  {r.get('Q1','—')}")
    for r in results:
        if r["Driver"]["familyName"]=="Leclerc":
            lines.append(f"\n\n🔴 <b>Шарль Леклер — Scuderia Ferrari</b>")
            lines.append(f"  Позиция: P{r['position']}")
            lines.append(f"  Q1: {r.get('Q1','—')} · Q2: {r.get('Q2','—')} · Q3: {r.get('Q3','—')}")
    return "\n".join(lines)

async def fmt_race():
    rname,results=await last_race()
    if not results: return "❌ Результаты гонки недоступны — сезон ещё не начался"
    stds=await driver_standings(); pm={s["Driver"]["driverId"]:s["points"] for s in stds}
    lines=[f"🏁 <b>Гонка — {rname}</b>\n"]; medals=["🥇","🥈","🥉"]
    lines.append("<b>Подиум:</b>")
    for r in results[:3]:
        d=r["Driver"]
        lines.append(f"  {medals[int(r['position'])-1]} <b>{d['givenName']} {d['familyName']}</b> ({r['Constructor']['name']}) +{r.get('points','0')} очк.")
    lines.append("\n<b>Итоговая таблица (топ-10):</b>")
    for r in results[:10]:
        d=r["Driver"]; total=pm.get(d["driverId"],"—")
        lines.append(f"  {r['position']}. <b>{d['givenName'][0]}. {d['familyName']}</b> ({r['Constructor']['name']}) +{r.get('points','0')} → {total} очк.")
    wks=build_weekends(); now=datetime.now(tz=pytz.utc)
    nxt=next((w for w in wks if w["end_utc"]>now),None)
    if nxt:
        rs=next((s for s in nxt["sessions"] if "гонка" in s["summary"].lower()),None)
        if rs: lines.append(f"\n📍 Следующая гонка: {nxt['flag']} <b>{nxt['gp_name']}</b>"); lines.append(f"  🕐 {dtstr(rs['start_utc'])}")
    for r in results:
        if r["Driver"]["familyName"]=="Leclerc":
            d=r["Driver"]; total=pm.get(d["driverId"],"—")
            lines.append(f"\n\n🔴 <b>Шарль Леклер — Scuderia Ferrari</b>")
            lines.append(f"  Финиш: P{r['position']}")
            lines.append(f"  Очки гонки: +{r.get('points','0')}")
            lines.append(f"  Итого в сезоне: {total} очк.")
            st=r.get("status","")
            if st and st!="Finished": lines.append(f"  ⚠️ Статус: {st}")
            if r.get("FastestLap"):
                fl=r["FastestLap"]; lines.append(f"  ⚡ Быстрый круг: {fl['Time']['time']} (круг {fl['lap']})")
    return "\n".join(lines)

# ── LIVE MONITOR ──────────────────────────────────────────────────────────────
class LiveMonitor:
    def __init__(self, sk, gp, city, sess_type="race"):
        self.key       = sk
        self.gp        = gp
        self.city      = city
        self.sess_type = sess_type   # "race" | "quali" | "practice"
        self.running   = False
        self.drivers: dict = {}
        self.seen_rc:  set = set()   # uid уже виденных race_control событий
        self.seen_pit: set = set()
        self.best_lap: Optional[float] = None
        # флаги чтобы не дублировать уникальные события
        self.form_sent    = False
        self.start_sent   = False
        self.podium_sent: set = set()   # P1, P2, P3

    # ── запуск ───────────────────────────────────────────────────────────────
    async def run(self):
        self.running = True
        self.drivers = await f1_drivers(self.key)
        log.info("🔴 LiveMonitor start sk=%s  %s  type=%s", self.key, self.gp, self.sess_type)

        # --- ВАЖНО: «проглатываем» всю историческую очередь без отправки ---
        history = await f1_rc(self.key)
        for m in history:
            uid = f"{m.get('date','')}{m.get('message','')}"
            self.seen_rc.add(uid)
        hist_pits = await f1_pit(self.key)
        for p in hist_pits:
            pid = f"{p.get('driver_number')}_{p.get('lap_number')}_{p.get('pit_duration')}"
            self.seen_pit.add(pid)
        # Лучший круг из истории (не отправляем, только запоминаем)
        hist_laps = await f1_laps(self.key)
        valid = [l for l in hist_laps if l.get("lap_duration") and l["lap_duration"] > 0]
        if valid:
            self.best_lap = min(l["lap_duration"] for l in valid)
        log.info("История загружена: %d RC, %d pit, best_lap=%s", len(self.seen_rc), len(self.seen_pit), self.best_lap)
        # ---------------------------------------------------------------------

        while self.running:
            try:
                await self._poll()
            except Exception as e:
                log.error("LiveMonitor poll: %s", e)
            await asyncio.sleep(10)

    def stop(self):
        self.running = False
        log.info("LiveMonitor stopped — %s", self.gp)

    def _name(self, n) -> str:
        return self.drivers.get(n, {}).get("full_name") or f"#{n}"
    def _team(self, n) -> str:
        return self.drivers.get(n, {}).get("team_name") or ""

    async def _bcast(self, text: str):
        if not _app: return
        for cid in active_subs():
            try:
                await _app.bot.send_message(cid, text, parse_mode="HTML")
            except Exception as e:
                log.warning("Live→%s: %s", cid, e)

    async def _poll(self):
        await self._rc()
        if self.sess_type == "race":
            await self._pitstop()
            await self._fl()
            await self._positions()
        elif self.sess_type == "quali":
            await self._fl()   # быстрые круги важны на квали

    # ── Race Control сообщения ───────────────────────────────────────────────
    async def _rc(self):
        msgs = await f1_rc(self.key)
        for m in msgs:
            uid = f"{m.get('date','')}{m.get('message','')}"
            if uid in self.seen_rc:
                continue
            self.seen_rc.add(uid)

            text = m.get("message", "")
            flag = (m.get("flag") or "").upper()
            cat  = (m.get("category") or "").upper()
            up   = text.upper()

            # ── ПРОГРЕВОЧНЫЙ КРУГ ────────────────────────────────────────────
            if ("FORMATION LAP" in up or "FORMATION" in up) and not self.form_sent:
                self.form_sent = True
                label = "Прогревочный круг" if self.sess_type == "race" else "Сессия начинается"
                await self._bcast(f"🏎️ <b>{label}!</b>\n\n<b>{self.gp}</b>")

            # ── СТАРТ ────────────────────────────────────────────────────────
            elif flag == "GREEN" and not self.start_sent and self.sess_type == "race":
                self.start_sent = True
                w = await get_weather(self.city)
                await self._bcast(
                    f"🚦 <b>ГОНКА СТАРТОВАЛА!</b>\n\n<b>{self.gp}</b>\n\n{fmt_weather(w)}"
                )

            elif ("GREEN LIGHT" in up or "RACE START" in up or "GO" == up.strip()) and not self.start_sent and self.sess_type == "race":
                self.start_sent = True
                w = await get_weather(self.city)
                await self._bcast(
                    f"🚦 <b>ГОНКА СТАРТОВАЛА!</b>\n\n<b>{self.gp}</b>\n\n{fmt_weather(w)}"
                )

            # ── SAFETY CAR ────────────────────────────────────────────────────
            elif "SAFETY CAR" in up and "VIRTUAL" not in up and "MEDICAL" not in up:
                if "DEPLOYED" in up or flag == "SC":
                    await self._bcast(f"🚗 <b>Safety Car!</b>\n\n<b>{self.gp}</b>\n{text}")
                elif "WITHDRAWN" in up or "IN THIS LAP" in up:
                    await self._bcast("🚗 <b>Safety Car возвращается в боксы</b>")

            # ── VIRTUAL SC ───────────────────────────────────────────────────
            elif flag == "VSC" or "VIRTUAL SAFETY CAR" in up:
                if "DEPLOYED" in up or flag == "VSC":
                    await self._bcast("🔶 <b>Virtual Safety Car (VSC)!</b>")
                elif "ENDING" in up or "RESUMED" in up:
                    await self._bcast("🔶 <b>VSC заканчивается — готовьтесь к рестарту!</b>")

            # ── ЖЁЛТЫЙ ────────────────────────────────────────────────────────
            elif flag == "YELLOW":
                sector = m.get("scope", "")
                await self._bcast(f"🟡 <b>Жёлтый флаг!</b>{' Сектор: ' + sector if sector else ''}\n{text}")

            # ── КРАСНЫЙ ───────────────────────────────────────────────────────
            elif flag == "RED":
                await self._bcast(f"🔴 <b>КРАСНЫЙ ФЛАГ! Сессия остановлена!</b>\n\n<b>{self.gp}</b>\n{text}")

            # ── ШТРАФ ─────────────────────────────────────────────────────────
            elif cat == "OTHER" and ("PENALTY" in up or "SANCTION" in up or "TIME PENALTY" in up):
                await self._bcast(f"⚖️ <b>Штраф / расследование</b>\n{text}")

            elif "DRIVE THROUGH" in up or "STOP AND GO" in up or "GRID PENALTY" in up:
                await self._bcast(f"⚖️ <b>Штраф!</b>\n{text}")

            # ── ФИНИШ ─────────────────────────────────────────────────────────
            elif flag == "CHEQUERED" or "CHEQUERED" in up or "CHECKERED" in up:
                label = "Гонка" if self.sess_type == "race" else "Сессия"
                await self._bcast(f"🏁 <b>{label} завершена!</b>\n\n<b>{self.gp}</b>")
                await asyncio.sleep(120)
                self.stop()
                return

    # ── Пит-стопы ────────────────────────────────────────────────────────────
    async def _pitstop(self):
        pits = await f1_pit(self.key)
        for p in pits:
            dur = p.get("pit_duration")
            if not dur:
                continue
            pid = f"{p.get('driver_number')}_{p.get('lap_number')}_{dur}"
            if pid in self.seen_pit:
                continue
            self.seen_pit.add(pid)
            dn = p.get("driver_number")
            await self._bcast(
                f"🔧 <b>Пит-стоп!</b>\n\n"
                f"👤 <b>{self._name(dn)}</b>\n"
                f"🏭 {self._team(dn)}\n"
                f"📌 Круг {p.get('lap_number','?')}\n"
                f"⏱ {float(dur):.1f} с в боксах"
            )

    # ── Быстрый круг ─────────────────────────────────────────────────────────
    async def _fl(self):
        # Запрашиваем только последние круги, не всю историю
        laps = await oget(f"/laps?session_key={self.key}&lap_number>1")
        valid = [l for l in laps if l.get("lap_duration") and 60 < l["lap_duration"] < 180]
        if not valid:
            return
        fastest = min(valid, key=lambda l: l["lap_duration"])
        dur = fastest["lap_duration"]
        if self.best_lap is not None and dur >= self.best_lap:
            return
        self.best_lap = dur
        dn   = fastest["driver_number"]
        mins = int(dur // 60)
        secs = dur % 60
        await self._bcast(
            f"⚡ <b>Новый быстрый круг!</b>\n\n"
            f"👤 <b>{self._name(dn)}</b> ({self._team(dn)})\n"
            f"⏱ <b>{mins}:{secs:06.3f}</b>\n"
            f"📌 Круг {fastest.get('lap_number','?')}"
        )

    # ── Позиции — следим за P1/P2/P3 финишем ────────────────────────────────
    async def _positions(self):
        if len(self.podium_sent) >= 3:
            return   # уже отправили топ-3
        pos_data = await oget(f"/position?session_key={self.key}")
        if not pos_data:
            return
        # Группируем по гонщику — берём самую последнюю запись
        latest: dict = {}
        for p in pos_data:
            dn = p.get("driver_number")
            latest[dn] = p
        # Ищем финишировавших на P1/P2/P3 (позиция не меняется + круг > 50)
        for dn, p in latest.items():
            position = p.get("position")
            if position in (1, 2, 3):
                key = f"P{position}"
                if key not in self.podium_sent:
                    # Отправляем только если круг достаточно поздний (≥ 50)
                    lap = p.get("lap_number", 0) or 0
                    if lap >= 50:
                        medals = {1:"🥇",2:"🥈",3:"🥉"}
                        self.podium_sent.add(key)
                        await self._bcast(
                            f"{medals[position]} <b>P{position} — {self._name(dn)}!</b>\n"
                            f"🏭 {self._team(dn)}\n"
                            f"📌 Круг {lap}"
                        )


_monitor: Optional[LiveMonitor] = None

async def start_live(gp: str, city: str, sess_type: str = "race"):
    global _monitor
    sess = await f1_latest_sess()
    if not sess:
        log.warning("start_live: нет OpenF1 сессии для %s", gp)
        return
    if _monitor and _monitor.running:
        _monitor.stop()
    _monitor = LiveMonitor(sess["session_key"], gp, city, sess_type)
    asyncio.create_task(_monitor.run())

# ── SCHEDULER ─────────────────────────────────────────────────────────────────
async def send_reminder(ev,mins):
    city=ev.get("city",ev["location"].split(",")[0].strip())
    msk_dt=_msk(ev["start_utc"]); loc=ev["start_utc"].astimezone(local_tz(city))
    if   mins==0:  head=f"{ev['emoji']} <b>СТАРТ ПРЯМО СЕЙЧАС!</b>"
    elif mins==5:  head=f"{ev['emoji']} <b>До старта 5 минут!</b>"
    else:          head=f"{ev['emoji']} <b>До старта 30 минут!</b>"
    lines=[head,"",f"{ev.get('flag','🏁')} <b>{ev['summary']}</b>",
           f"📍 {ev.get('country','')}, {city}",
           f"🕐 <b>{msk_dt.strftime('%H:%M')} МСК</b>  ·  {loc.strftime('%H:%M')} местного",""]
    w=await get_weather(city); lines.append(fmt_weather(w))
    if "гонка" in ev["summary"].lower() and mins>0:
        _,qr=await last_quali()
        if qr:
            lines.append("\n🏁 <b>Стартовая решётка:</b>")
            for q in qr[:5]: d=q["Driver"]; lines.append(f"  {q['position']}. {d['givenName'][0]}. {d['familyName']}")
            lines.append("  …")
    text="\n".join(lines)
    if not _app: return
    for cid in active_subs():
        try: await _app.bot.send_message(cid,text,parse_mode="HTML")
        except Exception as e: log.warning("Reminder→%s: %s",cid,e)

def schedule_all():
    wks=build_weekends(); now=datetime.now(tz=pytz.utc); count=0
    for w in wks:
        for s in w["sessions"]:
            start=s["start_utc"]; is_race="гонка" in s["summary"].lower()
            ev={**s,"gp_name":w["gp_name"],"city":w["city"],"country":w["country"],"flag":w["flag"],"location":s["location"]}
            for m in [30,5,0]:
                t=start-timedelta(minutes=m)
                if t>now:
                    scheduler.add_job(send_reminder,"date",run_date=t,args=[ev,m],
                                      id=f"r{m}_{s['summary']}_{start.isoformat()}",replace_existing=True)
                    count+=1
            # Запускаем live-монитор для гонки, квалификации и практики
            is_quali   = "квалификация" in s["summary"].lower() and "спринту" not in s["summary"].lower()
            is_sprint  = "спринт" in s["summary"].lower()
            is_practice= "свободных" in s["summary"].lower()
            if (is_race or is_quali or is_sprint or is_practice) and start>now:
                t_live = start + timedelta(minutes=2)
                if is_race or is_sprint:
                    stype = "race"
                elif is_quali:
                    stype = "quali"
                else:
                    stype = "practice"
                scheduler.add_job(start_live,"date",run_date=t_live,
                                  args=[w["gp_name"],w["city"],stype],
                                  id=f"live_{s['summary']}_{start.isoformat()}",replace_existing=True)
    log.info("Запланировано %d напоминаний, %d уикендов",count,len(wks))

# ── KEYBOARDS ─────────────────────────────────────────────────────────────────
REPLY_KB=ReplyKeyboardMarkup([["🏠 Меню"]],resize_keyboard=True,is_persistent=True)
def _home(): return [InlineKeyboardButton("🏠 Главное меню",callback_data="home")]

def main_kb(cid,priv):
    rows=[]
    if priv:
        subbed=is_subscribed(cid); enabled=subbed and not is_muted(cid)
        label=("🔔 Включить уведомления" if not subbed
               else "✅ Уведомления включены" if enabled else "☑️ Уведомления выключены")
        rows.append([InlineKeyboardButton(label,callback_data="notif_toggle")])
    rows+=[
        [InlineKeyboardButton("🏁 Текущий уикенд",callback_data="cal:current"),
         InlineKeyboardButton("⏭ Следующий уикенд",callback_data="cal:next")],
        [InlineKeyboardButton("⚡ Ближайшее событие",callback_data="cal:next_sess"),
         InlineKeyboardButton("🗓 Календарь по месяцам",callback_data="cal:months")],
        [InlineKeyboardButton("🏆 Чемпионат",callback_data="res:standings"),
         InlineKeyboardButton("📊 Последние результаты",callback_data="res:menu")],
    ]
    return InlineKeyboardMarkup(rows)

def months_kb(wks):
    seen=OrderedDict()
    for w in wks:
        m=_msk(w["start_utc"]); seen[(m.year,m.month)]=MR[m.month]
    rows=[]; row=[]
    for (y,mo),name in seen.items():
        row.append(InlineKeyboardButton(f"{name[:3]} {y}",callback_data=f"cal:month:{y}-{mo:02d}"))
        if len(row)==3: rows.append(row); row=[]
    if row: rows.append(row)
    rows.append(_home()); return InlineKeyboardMarkup(rows)

def month_nav_kb(wks,year,month):
    all_m=sorted({(_msk(w["start_utc"]).year,_msk(w["start_utc"]).month) for w in wks})
    idx=all_m.index((year,month)) if (year,month) in all_m else 0
    nav=[]
    if idx>0: py,pm=all_m[idx-1]; nav.append(InlineKeyboardButton(f"◀️ {MR[pm][:3]}",callback_data=f"cal:month:{py}-{pm:02d}"))
    if idx<len(all_m)-1: ny,nm=all_m[idx+1]; nav.append(InlineKeyboardButton(f"{MR[nm][:3]} ▶️",callback_data=f"cal:month:{ny}-{nm:02d}"))
    rows=[nav] if nav else []
    rows.append([InlineKeyboardButton("📅 Все месяцы",callback_data="cal:months")])
    rows.append(_home()); return InlineKeyboardMarkup(rows)

def back_kb(): return InlineKeyboardMarkup([_home()])
def results_kb(): return InlineKeyboardMarkup([[InlineKeyboardButton("🔥 Квалификация",callback_data="res:quali"),InlineKeyboardButton("🏁 Гонка",callback_data="res:race")],_home()])

# ── HANDLERS ──────────────────────────────────────────────────────────────────
async def cmd_start(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    chat=upd.effective_chat; priv=chat.type=="private"; subscribe(chat.id,priv)
    desc=("⏰ Напоминания <b>за 30 мин</b> и <b>за 5 мин</b> до каждой сессии\n"
          "🌡 Погода + местное время в напоминаниях\n"
          "🔴 Live-события во время гонки\n"
          "🏆 Результаты и чемпионат\n\n"
          "Нажми <b>🔔 Включить уведомления</b> чтобы получать их!" if priv
          else "Этот чат добавлен — уведомления будут приходить сюда!\n\nИспользуй /start для меню.")
    await upd.message.reply_text("🏠",reply_markup=REPLY_KB)
    await upd.message.reply_text(f"🏎️ <b>F1 2026 — бот уведомлений</b>\n\n{desc}",
                                  parse_mode="HTML",reply_markup=main_kb(chat.id,priv))

async def on_menu(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    chat=upd.effective_chat; priv=chat.type=="private"
    await upd.message.reply_text("🏎️ <b>F1 2026 — главное меню</b>",parse_mode="HTML",reply_markup=main_kb(chat.id,priv))

async def cmd_status(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    d=_load(); now=datetime.now(tz=MSK); live="✅ активен" if (_monitor and _monitor.running) else "💤 не активен"
    await upd.message.reply_text(f"📊 <b>Статус бота</b>\n\n👤 Личных: {len(d['users'])}\n💬 Чатов: {len(d['chats'])}\n🔕 Выключили: {len(d.get('muted',[]))}\n⏰ Напоминаний: {len(scheduler.get_jobs())}\n🔴 Live: {live}\n🕐 {now.strftime('%d.%m.%Y %H:%M')} МСК",parse_mode="HTML")

async def on_cb(upd:Update,ctx:ContextTypes.DEFAULT_TYPE):
    q:CallbackQuery=upd.callback_query; await q.answer()
    data=q.data; chat=upd.effective_chat; priv=chat.type=="private"

    if data=="home":
        await q.edit_message_text("🏎️ <b>F1 2026 — главное меню</b>",parse_mode="HTML",reply_markup=main_kb(chat.id,priv)); return

    if data=="notif_toggle" and priv:
        enabled=toggle_notif(chat.id,True)
        await q.answer("✅ Уведомления включены!" if enabled else "☑️ Уведомления выключены",show_alert=True)
        await q.edit_message_reply_markup(reply_markup=main_kb(chat.id,True)); return

    if data=="cal:current":
        wks=build_weekends(); w=cur_weekend(wks) or nxt_weekend(wks)
        if not w: await q.edit_message_text("😔 Нет данных.",reply_markup=back_kb()); return
        await q.edit_message_text("⏳ Загружаю...",parse_mode="HTML")
        text=await fmt_cur_weekend(w); await q.edit_message_text(text[:4090],parse_mode="HTML",reply_markup=back_kb()); return

    if data=="cal:next":
        w=nxt_weekend(build_weekends())
        if not w: await q.edit_message_text("😔 Нет предстоящих уикендов.",reply_markup=back_kb()); return
        await q.edit_message_text(fmt_card(w),parse_mode="HTML",reply_markup=back_kb()); return

    if data=="cal:next_sess":
        s=nxt_session(build_weekends())
        if not s: await q.edit_message_text("😔 Нет предстоящих событий.",reply_markup=back_kb()); return
        await q.edit_message_text("⏳ Загружаю погоду...",parse_mode="HTML")
        text=await fmt_next_sess(s); await q.edit_message_text(text[:4090],parse_mode="HTML",reply_markup=back_kb()); return

    if data=="cal:months":
        wks=build_weekends()
        await q.edit_message_text("📅 <b>Выбери месяц:</b>",parse_mode="HTML",reply_markup=months_kb(wks)); return

    if data.startswith("cal:month:"):
        ym=data.split(":",2)[2]; year=int(ym.split("-")[0]); month=int(ym.split("-")[1])
        wks=build_weekends(); text=fmt_month(wks,year,month)
        await q.edit_message_text(text,parse_mode="HTML",reply_markup=month_nav_kb(wks,year,month)); return

    if data=="res:standings":
        await q.edit_message_text("⏳ Загружаю...",parse_mode="HTML")
        await q.edit_message_text((await fmt_standings())[:4090],parse_mode="HTML",reply_markup=back_kb()); return

    if data=="res:menu":
        await q.edit_message_text("📊 <b>Выбери тип результатов:</b>",parse_mode="HTML",reply_markup=results_kb()); return

    if data=="res:quali":
        await q.edit_message_text("⏳ Загружаю...",parse_mode="HTML")
        await q.edit_message_text((await fmt_quali())[:4090],parse_mode="HTML",reply_markup=back_kb()); return

    if data=="res:race":
        await q.edit_message_text("⏳ Загружаю...",parse_mode="HTML")
        await q.edit_message_text((await fmt_race())[:4090],parse_mode="HTML",reply_markup=back_kb()); return

# ── MAIN ──────────────────────────────────────────────────────────────────────
async def post_init(app:Application):
    global http,_app
    http=aiohttp.ClientSession(); _app=app
    schedule_all(); scheduler.start()
    log.info("🏎️  F1 2026 Bot запущен!")

async def post_shutdown(app:Application):
    if _monitor and _monitor.running: _monitor.stop()
    if http and not http.closed: await http.close()
    if scheduler.running: scheduler.shutdown(wait=False)

def main():
    app=(Application.builder().token(BOT_TOKEN).post_init(post_init).post_shutdown(post_shutdown).build())
    app.add_handler(CommandHandler("start",cmd_start))
    app.add_handler(CommandHandler("status",cmd_status))
    app.add_handler(CallbackQueryHandler(on_cb))
    app.add_handler(MessageHandler(filters.Text(["🏠 Меню"]),on_menu))
    app.run_polling(drop_pending_updates=True)

if __name__=="__main__":
    main()
