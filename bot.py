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

async def broadcast(text: str, parse_mode: str = "HTML"):
    """Рассылка всем подписчикам с обработкой ошибок и автоочисткой мёртвых чатов."""
    if not _app:
        return
    dead = []
    migrated = []   # (old_id, new_id)
    for cid in active_subs():
        try:
            await asyncio.wait_for(
                    _app.bot.send_message(cid, text, parse_mode=parse_mode),
                    timeout=10.0)
        except Exception as e:
            err = str(e)
            if "ChatMigrated" in err or "chat_id_invalid" in err.lower():
                # Группа стала супергруппой — пробуем найти новый ID из ошибки
                import re as _re
                m = _re.search(r"migrate_to_chat_id.*?(-[0-9]+)", err)
                new_id = int(m.group(1)) if m else None
                if new_id:
                    migrated.append((cid, new_id))
                    try:
                        await asyncio.wait_for(
                                _app.bot.send_message(new_id, text, parse_mode=parse_mode),
                                timeout=10.0)
                        log.info("ChatMigrated: %s → %s, обновляем", cid, new_id)
                    except Exception as e2:
                        log.warning("После миграции %s: %s", new_id, e2)
                else:
                    dead.append(cid)
            elif any(x in err for x in ["Forbidden", "bot was kicked", "user is deactivated",
                                         "chat not found", "blocked"]):
                dead.append(cid)
                log.info("Мёртвый чат убран: %s (%s)", cid, err[:60])
            else:
                log.warning("Broadcast→%s: %s", cid, err)

    # Обновляем storage
    if dead or migrated:
        d = _load()
        for cid in dead:
            for key in ("users", "chats", "muted"):
                if cid in d.get(key, []): d[key].remove(cid)
        for old_id, new_id in migrated:
            for key in ("users", "chats"):
                if old_id in d.get(key, []):
                    d[key].remove(old_id)
                    if new_id not in d[key]: d[key].append(new_id)
            if old_id in d.get("muted", []):
                d["muted"].remove(old_id)
        _save(d)

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

def _weekend_end_msk(w) -> datetime:
    """Уикенд считается «текущим» до понедельника 06:00 МСК после гонки."""
    race_start = w["end_utc"]   # end_utc = старт последней сессии (гонки)
    race_end   = race_start + timedelta(hours=4)   # гонка ~2ч + запас
    # Переводим в МСК, находим ближайший пн 06:00
    msk_end = race_end.astimezone(MSK)
    days_to_mon = (7 - msk_end.weekday()) % 7   # 0 если уже пн
    if days_to_mon == 0 and msk_end.hour >= 6:
        days_to_mon = 7
    mon_0600 = msk_end.replace(hour=6, minute=0, second=0, microsecond=0) + timedelta(days=days_to_mon)
    return mon_0600.astimezone(pytz.utc)

def cur_weekend(wks):
    now = datetime.now(tz=pytz.utc)
    for w in wks:
        window_start = w["start_utc"] - timedelta(hours=6)
        window_end   = _weekend_end_msk(w)
        if window_start <= now <= window_end:
            return w
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

async def get_weather(city, target_dt: datetime = None):
    """
    Погода с жёстким общим таймаутом 6 сек.
    wttr.in → open-meteo → пустой dict (не блокируем event loop).
    """
    city_q = _city_en(city)
    try:
        return await asyncio.wait_for(_get_weather_inner(city_q, target_dt), timeout=6.0)
    except asyncio.TimeoutError:
        log.warning("get_weather: таймаут 6с для '%s'", city_q)
        return {"current": {}, "forecast": None}
    except Exception as e:
        log.warning("get_weather: %s", e)
        return {"current": {}, "forecast": None}

async def _get_weather_inner(city_q: str, target_dt: datetime = None):
    # ── Попытка 1: wttr.in (один раз, таймаут 4 сек) ─────────────────────────
    data = None
    try:
        url = f"https://wttr.in/{city_q}?format=j1"
        async with http.get(url, timeout=aiohttp.ClientTimeout(total=4),
                            headers={"User-Agent": "curl/7.68.0"}) as r:
            text = await r.text()
            if text.strip().startswith("{"):
                import json as _json
                data = _json.loads(text)
    except Exception as e:
        log.debug("wttr.in для '%s': %s", city_q, e)

    if data:
        try:
            cur = data["current_condition"][0]
            cur_rain = max(int(h.get("chanceofrain", 0))
                           for day in data["weather"][:1]
                           for h in day["hourly"][:3])
            cur_w = {
                "temp":  cur["temp_C"],
                "feels": cur["FeelsLikeC"],
                "desc":  _wx(cur["weatherDesc"][0]["value"]),
                "hum":   cur["humidity"],
                "wind":  cur["windspeedKmph"],
                "rain":  cur_rain,
            }

            fc_w = None
            if target_dt is not None:
                now_utc    = datetime.now(tz=pytz.utc)
                delta_days = (target_dt.astimezone(pytz.utc) - now_utc).total_seconds() / 86400
                if 0 < delta_days <= 5:
                    target_date = target_dt.astimezone(pytz.utc).strftime("%Y-%m-%d")
                    target_hour = target_dt.astimezone(pytz.utc).hour
                    best_h, best_diff = None, 99
                    for day in data["weather"]:
                        if day.get("date", "") != target_date:
                            continue
                        for h in day["hourly"]:
                            diff = abs(int(h["time"]) // 100 - target_hour)
                            if diff < best_diff:
                                best_diff, best_h = diff, h
                    if best_h:
                        fc_w = {
                            "temp":  best_h["tempC"],
                            "feels": best_h["FeelsLikeC"],
                            "desc":  _wx(best_h["weatherDesc"][0]["value"]),
                            "hum":   best_h["humidity"],
                            "wind":  best_h["windspeedKmph"],
                            "rain":  int(best_h.get("chanceofrain", 0)),
                        }

            return {"current": cur_w, "forecast": fc_w}
        except Exception as e:
            log.warning("Парсинг wttr.in для '%s': %s", city_q, e)

    # ── Резерв: Open-Meteo ───────────────────────────────────────────────────
    log.debug("Переключаемся на open-meteo для '%s'", city_q)
    try:
        # Шаг 1: получаем координаты через open-meteo geocoding
        geo_url = f"https://geocoding-api.open-meteo.com/v1/search?name={city_q}&count=1&language=en"
        async with http.get(geo_url, timeout=aiohttp.ClientTimeout(total=8)) as r:
            geo = await r.json(content_type=None)
        if not geo.get("results"):
            raise ValueError(f"Город не найден: {city_q}")
        loc  = geo["results"][0]
        lat, lon = loc["latitude"], loc["longitude"]

        # Шаг 2: текущая погода + почасовой прогноз
        wx_url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            f"&current=temperature_2m,apparent_temperature,weathercode,windspeed_10m,relativehumidity_2m"
            f"&hourly=temperature_2m,apparent_temperature,weathercode,windspeed_10m,relativehumidity_2m,precipitation_probability"
            f"&forecast_days=5&timezone=UTC"
        )
        async with http.get(wx_url, timeout=aiohttp.ClientTimeout(total=8)) as r:
            wx = await r.json(content_type=None)

        def _wmo(code):
            # WMO weather code → описание
            WMO = {0:"☀️ Ясно",1:"🌤 Преимущественно ясно",2:"⛅ Переменная облачность",
                   3:"☁️ Пасмурно",45:"🌫 Туман",48:"🌫 Иней",51:"🌦 Лёгкая морось",
                   53:"🌦 Морось",55:"🌧 Сильная морось",61:"🌦 Лёгкий дождь",
                   63:"🌧 Дождь",65:"🌧 Сильный дождь",71:"🌨 Лёгкий снег",
                   73:"🌨 Снег",75:"❄️ Сильный снег",80:"🌦 Ливень",
                   81:"🌧 Сильный ливень",95:"⛈ Гроза",96:"⛈ Гроза с градом"}
            return WMO.get(int(code), f"Код {code}")

        cur_c = wx["current"]
        cur_w = {
            "temp":  str(round(cur_c["temperature_2m"])),
            "feels": str(round(cur_c["apparent_temperature"])),
            "desc":  _wmo(cur_c["weathercode"]),
            "hum":   str(round(cur_c["relativehumidity_2m"])),
            "wind":  str(round(cur_c["windspeed_10m"])),
            "rain":  0,
        }

        fc_w = None
        if target_dt is not None:
            now_utc    = datetime.now(tz=pytz.utc)
            delta_days = (target_dt.astimezone(pytz.utc) - now_utc).total_seconds() / 86400
            if 0 < delta_days <= 5:
                target_iso = target_dt.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:00")
                times = wx["hourly"]["time"]
                if target_iso in times:
                    idx = times.index(target_iso)
                else:
                    # ближайший час
                    from datetime import datetime as _dt
                    target_ts = target_dt.astimezone(pytz.utc).replace(minute=0, second=0, microsecond=0)
                    idx = min(range(len(times)),
                              key=lambda i: abs((_dt.fromisoformat(times[i]).replace(tzinfo=pytz.utc) - target_ts).total_seconds()))
                h = wx["hourly"]
                fc_w = {
                    "temp":  str(round(h["temperature_2m"][idx])),
                    "feels": str(round(h["apparent_temperature"][idx])),
                    "desc":  _wmo(h["weathercode"][idx]),
                    "hum":   str(round(h["relativehumidity_2m"][idx])),
                    "wind":  str(round(h["windspeed_10m"][idx])),
                    "rain":  int(h["precipitation_probability"][idx] or 0),
                }

        log.info("open-meteo успешно для '%s'", city_q)
        return {"current": cur_w, "forecast": fc_w}

    except Exception as e:
        log.error("open-meteo для '%s': %s", city_q, e)
        return {"current": {}, "forecast": None}


def fmt_weather_block(wdata: dict, target_dt: datetime = None) -> str:
    """Показывает прогноз на время сессии; если недоступен — текущую погоду."""
    cur = wdata.get("current", {})
    fc  = wdata.get("forecast")

    def _fmt(w, label):
        if not w:
            return f"{label}: данные недоступны"
        rain = f"☔ {w['rain']}%" if int(w["rain"]) > 5 else "☀️ без осадков"
        return (label + "\n"
                + f"{w['desc']}\n"
                + f"🌡 <b>{w['temp']}°C</b> (ощущается {w['feels']}°C)\n"
                + f"💧 {w['hum']}% · 💨 {w['wind']} км/ч · {rain}")

    # Если есть прогноз на время сессии — показываем только его
    if fc and target_dt:
        msk_t = target_dt.astimezone(MSK).strftime("%H:%M МСК")
        return _fmt(fc, f"🔮 <b>Прогноз погоды на {msk_t}:</b>")

    # Прогноз недоступен (> 5 дней) — показываем текущую
    return _fmt(cur, "🌡 <b>Погода сейчас:</b>")


# Обратная совместимость — старый fmt_weather для live-монитора
def fmt_weather(w):
    if isinstance(w, dict) and "current" in w:
        w = w.get("current", {})
    if not w: return "🌡 Погода временно недоступна"
    rain = f"☔ {w['rain']}%" if int(w["rain"]) > 5 else "☀️ Без осадков"
    return f"🌡 <b>{w['temp']}°C</b> (ощущается {w['feels']}°C) · {w['desc']}\n💧 {w['hum']}% · 💨 {w['wind']} км/ч · {rain}"

# ── JOLPICA API ───────────────────────────────────────────────────────────────
J="https://api.jolpi.ca/ergast/f1"
async def jget(path):
    for attempt in range(3):
        try:
            async with http.get(f"{J}{path}", timeout=aiohttp.ClientTimeout(total=12)) as r:
                if r.status == 200:
                    return await r.json(content_type=None)
        except Exception as e:
            log.debug("jget %s attempt %d: %s", path, attempt+1, e)
            if attempt < 2:
                await asyncio.sleep(1)
    return None

async def driver_standings():
    """
    1. Пробуем /current/driverStandings — официальный эндпоинт
    2. Fallback: строим сами из /current/last/results + всех раундов текущего года
    """
    # Попытка 1: официальная таблица
    d = await jget("/current/driverStandings.json")
    if d:
        try:
            lst = d["MRData"]["StandingsTable"]["StandingsLists"]
            if lst and lst[0].get("DriverStandings"):
                return lst[0]["DriverStandings"]
        except Exception as e:
            log.warning("driverStandings parse error: %s", e)

    # Попытка 2: строим из результатов
    return await _standings_from_results()

async def _standings_from_results() -> list:
    """
    Строим таблицу чемпионата из тех же данных что уже работают в боте.
    Используем /current/last/results.json (работает всегда если есть хоть одна гонка),
    затем добираем предыдущие раунды через /current/{n}/results.json.
    """
    year = datetime.now(tz=pytz.utc).year

    # Шаг 1: last_race() — тот же вызов что работает в "Этот уикенд"
    rname, rdate, last_results = await last_race()
    if not last_results:
        log.warning("_standings_from_results: last_race пустая")
        return []

    # Узнаём номер последнего раунда
    d_last = await jget("/current/last/results.json")
    last_rnd = 1
    if d_last:
        try:
            last_rnd = int(d_last["MRData"]["RaceTable"].get("round", "1"))
        except Exception:
            pass

    log.info("_standings_from_results: последний раунд %d, строим таблицу", last_rnd)

    # Шаг 2: параллельно тянем все раунды по номеру
    if last_rnd == 1:
        all_rounds = [last_results]
    else:
        tasks = [jget(f"/{year}/{rnd}/results.json") for rnd in range(1, last_rnd + 1)]
        raws  = await asyncio.gather(*tasks, return_exceptions=True)
        all_rounds = []
        for raw in raws:
            if isinstance(raw, Exception) or not raw:
                continue
            try:
                races = raw["MRData"]["RaceTable"]["Races"]
                for race in races:
                    all_rounds.append(race.get("Results", []))
            except Exception:
                continue
        if not all_rounds:
            # Если ничего не получили — используем хотя бы last_race
            all_rounds = [last_results]

    # Шаг 3: агрегируем очки
    pts: dict = {}
    for results in all_rounds:
        for r in results:
            d_obj = r["Driver"]
            did   = d_obj["driverId"]
            p     = float(r.get("points", 0) or 0)
            if did not in pts:
                pts[did] = {
                    "Driver":       d_obj,
                    "Constructors": [r.get("Constructor", {})],
                    "points":       0.0,
                    "wins":         0,
                }
            pts[did]["points"] += p
            if r.get("position") == "1":
                pts[did]["wins"] += 1
            pts[did]["Constructors"] = [r.get("Constructor", {})]

    sorted_d = sorted(pts.values(), key=lambda x: -x["points"])
    for i, s in enumerate(sorted_d, 1):
        s["position"] = str(i)
        p = s["points"]
        s["points"] = str(int(p) if p == int(p) else p)
        s["wins"]   = str(s["wins"])

    log.info("_standings_from_results: готово, %d гонщиков", len(sorted_d))
    return sorted_d

async def last_quali():
    """Возвращает (raceName, date_str, results)."""
    d=await jget("/current/last/qualifying.json")
    if not d: return "","",[]
    races=d["MRData"]["RaceTable"]["Races"]
    if not races: return "","",[]
    r = races[0]
    return r.get("raceName",""), r.get("date",""), r.get("QualifyingResults",[])

async def last_race():
    """Возвращает (raceName, date_str, results)."""
    d=await jget("/current/last/results.json")
    if not d: return "","",[]
    races=d["MRData"]["RaceTable"]["Races"]
    if not races: return "","",[]
    r = races[0]
    return r.get("raceName",""), r.get("date",""), r.get("Results",[])

async def race_by_round(season: int, rnd: int):
    d=await jget(f"/{season}/{rnd}/results.json")
    if not d: return "",[]
    races=d["MRData"]["RaceTable"]["Races"]
    if not races: return "",[]
    return races[0].get("raceName",""),races[0].get("Results",[])

async def quali_by_round(season: int, rnd: int):
    d=await jget(f"/{season}/{rnd}/qualifying.json")
    if not d: return "",[]
    races=d["MRData"]["RaceTable"]["Races"]
    if not races: return "",[]
    return races[0].get("raceName",""),races[0].get("QualifyingResults",[])

# ── API LAYER: OpenF1 + F1LiveTiming (параллельно) ───────────────────────────
O  = "https://api.openf1.org/v1"
MV = "https://api.multiviewer.app/api/v1"

_TIMEOUT_FAST = aiohttp.ClientTimeout(total=5)
_TIMEOUT_SLOW = aiohttp.ClientTimeout(total=10)

async def _get_json(url: str, timeout=None) -> list | dict | None:
    """Быстрый GET с таймаутом, возвращает None при ошибке."""
    try:
        async with http.get(url, timeout=timeout or _TIMEOUT_FAST) as r:
            if r.status == 200:
                return await r.json(content_type=None)
    except Exception as e:
        log.debug("GET %s: %s", url, e)
    return None

async def oget(path) -> list:
    """OpenF1 GET → всегда список."""
    result = await _get_json(f"{O}{path}")
    if isinstance(result, list): return result
    return []

async def mvget(path) -> dict | None:
    """Multiviewer GET → dict или None."""
    return await _get_json(f"{MV}{path}")

# ── Параллельный fetch с race-condition: берём первый непустой ответ ──────────
async def _parallel_first(*coros):
    """Запускает корутины параллельно, возвращает первый непустой результат."""
    tasks = [asyncio.ensure_future(c) for c in coros]
    result = None
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            val = task.result()
            if val:
                result = val
                break
        # если первый пустой — ждём остальных
        if not result and pending:
            done2, _ = await asyncio.wait(pending, timeout=4)
            for task in done2:
                val = task.result()
                if val:
                    result = val
                    break
    finally:
        for t in tasks:
            if not t.done(): t.cancel()
    return result

async def f1_latest_sess():
    d = await oget("/sessions?session_key=latest")
    return d[0] if d else None

async def f1_drivers(sk: int) -> dict:
    d = await oget(f"/drivers?session_key={sk}")
    return {x["driver_number"]: x for x in d}

# ── Race Control: OpenF1 основной, Multiviewer запасной ──────────────────────
async def _rc_openf1(sk: int) -> list:
    return await oget(f"/race_control?session_key={sk}")

async def _rc_multiviewer(year: int, session_path: str) -> list:
    """
    Multiviewer хранит Race Control в SessionInfo.
    Возвращает список событий в формате совместимом с OpenF1.
    """
    data = await mvget(f"/session/{year}/{session_path}/RaceControlMessages")
    if not data or not isinstance(data, dict):
        return []
    messages = data.get("Messages", {})
    result = []
    for key, m in messages.items():
        if not isinstance(m, dict):
            continue
        result.append({
            "date":    m.get("Utc", ""),
            "message": m.get("Message", ""),
            "flag":    m.get("Flag", ""),
            "category":m.get("Category", ""),
            "scope":   m.get("Scope", ""),
        })
    return result

async def f1_rc(sk: int, mv_path: str = None, year: int = 2026) -> list:
    """Параллельно тянет Race Control из OpenF1 и Multiviewer, мёржит."""
    coros = [_rc_openf1(sk)]
    if mv_path:
        coros.append(_rc_multiviewer(year, mv_path))
    results = await asyncio.gather(*coros, return_exceptions=True)

    merged, seen_uid = [], set()
    for batch in results:
        if isinstance(batch, Exception) or not batch:
            continue
        for m in batch:
            uid = f"{m.get('date','')}{m.get('message','')}"
            if uid not in seen_uid:
                seen_uid.add(uid)
                merged.append(m)
    return sorted(merged, key=lambda x: x.get("date", ""))

async def f1_pit(sk: int) -> list:
    return await oget(f"/pit?session_key={sk}")

async def f1_laps(sk: int) -> list:
    """Только круги с реальным временем (60–200 сек)."""
    laps = await oget(f"/laps?session_key={sk}")
    return [l for l in laps if l.get("lap_duration") and 60 < l["lap_duration"] < 200]

async def f1_positions(sk: int) -> list:
    return await oget(f"/position?session_key={sk}")

# ── DRIVER COUNTRY FLAGS ─────────────────────────────────────────────────────
_DRIVER_FLAG = {
    # 2026 grid + recent drivers
    "Verstappen":"🇳🇱","Hamilton":"🇬🇧","Leclerc":"🇲🇨","Norris":"🇬🇧",
    "Piastri":"🇦🇺","Russell":"🇬🇧","Sainz":"🇪🇸","Alonso":"🇪🇸",
    "Perez":"🇲🇽","Stroll":"🇨🇦","Gasly":"🇫🇷","Ocon":"🇫🇷",
    "Bottas":"🇫🇮","Zhou":"🇨🇳","Albon":"🇹🇭","Sargeant":"🇺🇸",
    "Hulkenberg":"🇩🇪","Magnussen":"🇩🇰","Tsunoda":"🇯🇵","De Vries":"🇳🇱",
    "Lawson":"🇳🇿","Bearman":"🇬🇧","Colapinto":"🇦🇷","Doohan":"🇦🇺",
    "Antonelli":"🇮🇹","Hadjar":"🇫🇷","Bortoleto":"🇧🇷","Iwasa":"🇯🇵",
}
def driver_emoji(last_name: str) -> str:
    return _DRIVER_FLAG.get(last_name, "🏁")

def fmt_driver(first: str, last: str, team: str = "", bold: bool = True) -> str:
    """Единый формат: 🇲🇨 <b>Charles Leclerc</b> (Ferrari)"""
    flag   = driver_emoji(last)
    name   = f"{first} {last}".strip()
    name_s = f"<b>{name}</b>" if bold else name
    team_s = f" ({team})" if team else ""
    return f"{flag} {name_s}{team_s}"

def fmt_driver_jolpica(d: dict, team: str = "", bold: bool = True) -> str:
    """Форматирует гонщика из Jolpica dict."""
    return fmt_driver(d.get("givenName",""), d.get("familyName",""), team, bold)

def fmt_result_row(pos: int | str, d: dict, team: str,
                   time_or_gap: str = "", points: str = "",
                   total_pts: str = "", bold: bool = True) -> str:
    """
    Чистая двухстрочная строка результата:
      3. 🇲🇨 Charles Leclerc (Ferrari)
         ⏱ +4.123  ·  +15 очк.  →  43 всего
    """
    drv   = fmt_driver_jolpica(d, team, bold=bold)
    stats = []
    if time_or_gap: stats.append(f"⏱ {time_or_gap}")
    if points not in ("","0","0.0"): stats.append(f"+{points} очк.")
    if total_pts:   stats.append(f"→ {total_pts} всего")
    stat_line = "  " + "  ·  ".join(stats) if stats else ""
    row = f"  {pos}. {drv}"
    if stats:
        row += "\n" + "     " + "  ·  ".join(stats)
    return row


# ── FORMATTERS ────────────────────────────────────────────────────────────────
def _msk(dt): return dt.astimezone(MSK)
def dtstr(dt): m=_msk(dt); return f"{m.day} {MG[m.month]}, {m.strftime('%H:%M')} МСК"

def fmt_card(w, show_done=True):
    """
    Карточка уикенда.
    show_done=True  — пройденные сессии помечаются ✓ и зачёркиваются.
    """
    now  = datetime.now(tz=pytz.utc)
    s    = _msk(w["start_utc"])
    e    = _msk(w["end_utc"])
    dates = (f"{s.day}–{e.day} {MG[s.month]}"
             if s.month == e.month
             else f"{s.day} {MG[s.month]} – {e.day} {MG[e.month]}")

    lines = [
        f"{w['flag']}  <b>{w['gp_name'].upper()}</b>",
        f"<i>📍 {w['country']}, {w['city']}   ·   {dates}</i>",
        "",
    ]

    for ss in w["sessions"]:
        dt      = _msk(ss["start_utc"])
        done    = show_done and ss["start_utc"] < now
        # Формат: две строки — заголовок, затем дата·время
        day_str  = f"{dt.day:02d} {MG[dt.month]}"
        time_str = dt.strftime("%H:%M")

        if done:
            # Зачёркнутый через unicode + серый индикатор
            name_s  = "".join(c + "̶" for c in ss["short"])
            lines.append(f"  ✓  <s>{ss['emoji']} {ss['short']}</s>")
            lines.append(f"      <s>📅 {day_str}  ·  🕐 {time_str} МСК</s>")
        else:
            lines.append(f"  {ss['emoji']}  <b>{ss['short']}</b>")
            lines.append(f"      📅 {day_str}  ·  🕐 {time_str} МСК")
        lines.append("")   # пустая строка между сессиями

    # убираем последнюю лишнюю пустую строку
    if lines and lines[-1] == "":
        lines.pop()

    return "\n".join(lines)

def fmt_month(wks,year,month):
    now   = datetime.now(tz=pytz.utc)
    mwks  = [w for w in wks if _msk(w["start_utc"]).year==year and _msk(w["start_utc"]).month==month]
    hdr   = f"🗓 <b>{MR[month]} {year}</b>  —  {len(mwks)} уикенда\n"
    sep   = "\n\n━━━━━━━━━━━━━━━━━━━━\n\n"
    cards = sep.join(fmt_card(w, show_done=True) for w in mwks)
    return (hdr + cards)[:4090]

async def fmt_stroll_stats() -> str:
    """
    Полная статистика Лэнса Стролла:
    — последняя гонка (позиция, старт, очки, сход, быстрый круг)
    — последняя квалификация
    — сезонный итог по всем прошедшим раундам
    """
    year = datetime.now(tz=pytz.utc).year
    now  = datetime.now(tz=pytz.utc)

    # Параллельно: последняя гонка + квали + все раунды для сезонной статистики
    (rname, _, race_results), (qname, _, q_results), stds = await asyncio.gather(
        last_race(), last_quali(), driver_standings(), return_exceptions=True
    )
    if isinstance(race_results, Exception): race_results = []
    if isinstance(q_results,   Exception): q_results    = []
    if isinstance(stds,        Exception): stds         = []

    lines = ["🟦 <b>Лэнс Стролл (Aston Martin) — статистика</b>\n"]

    # ── СЕЗОН (из таблицы чемпионата) ───────────────────────────────────────
    past = [w for w in build_weekends() if w["end_utc"] < now]
    stroll_season = next((s for s in stds if s["Driver"]["familyName"]=="Stroll"), None)

    if stroll_season:
        sp   = stroll_season
        spos = sp.get("position") or sp.get("positionText","—")
        spts = sp.get("points","0")
        swins= sp.get("wins","0")
        lines.append(f"📊 <b>Сезон 2026 — итог ({len(past)} гонок):</b>")
        lines.append(f"  🏆 Место в чемпионате: <b>P{spos}</b>")
        lines.append(f"  💎 Набрано очков: <b>{spts}</b>")
        if swins and swins != "0":
            lines.append(f"  🥇 Побед: {swins}")
    else:
        lines.append(f"📊 <b>Сезон 2026 ({len(past)} гонок)</b>")

    # Считаем детальную статистику по всем раундам
    if past:
        tasks   = [race_by_round(year, rnd) for rnd in range(1, len(past)+1)]
        rounds  = await asyncio.gather(*tasks, return_exceptions=True)

        dnf_count   = 0
        podiums     = 0
        points_list = []
        best_pos    = 99
        pos_history = []

        for res in rounds:
            if isinstance(res, Exception) or not res: continue
            _, results = res
            sr = next((r for r in results if r["Driver"]["familyName"]=="Stroll"), None)
            if not sr: continue
            pos    = int(sr.get("position", 99))
            pts    = float(sr.get("points", 0) or 0)
            status = sr.get("status","Finished")
            is_dnf = status not in ("Finished","+1 Lap","+2 Laps","+3 Laps","+3 Laps","+4 Laps")
            if is_dnf:    dnf_count += 1
            if pos <= 3:  podiums   += 1
            if pos < best_pos: best_pos = pos
            points_list.append(pts)
            pos_history.append((pos, is_dnf))

        avg_pts = sum(points_list)/len(points_list) if points_list else 0
        scored  = sum(1 for p in points_list if p > 0)

        lines.append(f"  📈 Очков в среднем за гонку: {avg_pts:.1f}")
        lines.append(f"  ✅ Финишей в очках: {scored} из {len(points_list)}")
        lines.append(f"  ❌ Сходов: {dnf_count}")
        if podiums: lines.append(f"  🏆 Подиумов: {podiums}")
        if best_pos < 99: lines.append(f"  ⭐ Лучший результат: P{best_pos}")

        # История позиций (компактно)
        if pos_history:
            hist = []
            for pos, dnf in pos_history:
                if dnf:      hist.append("❌")
                elif pos==1: hist.append("🥇")
                elif pos==2: hist.append("🥈")
                elif pos==3: hist.append("🥉")
                elif pos<=10:hist.append(f"P{pos}")
                else:        hist.append(f"<i>P{pos}</i>")
            lines.append(f"  📋 По гонкам: {' · '.join(hist)}")

        # ── Хуев в жопе (сезон) ──────────────────────────────────────────────
        total_ahead_season = 0
        total_participants_season = 0
        for res in rounds:
            if isinstance(res, Exception) or not res: continue
            _, rr = res
            if not rr: continue
            sr2 = next((r for r in rr if r["Driver"]["familyName"]=="Stroll"), None)
            if not sr2: continue
            stroll_pos2   = int(sr2.get("position", len(rr)) or len(rr))
            n_participants = len(rr)
            ahead          = stroll_pos2 - 1   # гонщики впереди Стролла
            total_ahead_season       += ahead
            total_participants_season += n_participants
        lines.append("")
        lines.append(f"  🍆 <b>Хуев в жопе (сезон): {total_ahead_season} из {total_participants_season}</b>")

    lines.append("")

    # ── ПОСЛЕДНЯЯ ГОНКА ──────────────────────────────────────────────────────
    sr = next((r for r in race_results if r["Driver"]["familyName"]=="Stroll"), None)
    if sr and rname:
        pm     = {s["Driver"]["driverId"]:s["points"] for s in stds}
        d      = sr["Driver"]
        pos    = int(sr.get("position",0) or 0)
        pts    = sr.get("points","0")
        total  = pm.get(d["driverId"],"—")
        status = sr.get("status","Finished")
        laps   = int(sr.get("laps","0") or 0)
        grid   = int(sr.get("grid","0") or 0)
        dnf    = status not in ("Finished","+1 Lap","+2 Laps","+3 Laps")

        qpos_map = {r["Driver"]["driverId"]: int(r["position"]) for r in q_results} if q_results else {}
        q_pos  = qpos_map.get(d["driverId"], grid)

        if q_pos and pos:
            delta = q_pos - pos
            if delta > 0:   chg = f"+{delta} ↑ (P{q_pos}→P{pos})"
            elif delta < 0: chg = f"{delta} ↓ (P{q_pos}→P{pos})"
            else:           chg = f"без изменений (P{pos})"
        else:
            chg = f"P{pos}"

        winner = race_results[0] if race_results else None
        gap_to_win = sr.get("Time",{}).get("time","") if pos > 1 else ""
        fl_data = sr.get("FastestLap")

        lines.append(f"🏁 <b>Последняя гонка: {rname}</b>")
        lines.append(f"  🏁 Старт: <b>P{grid}</b> (квали P{q_pos})  →  Финиш: <b>P{pos}</b>")
        lines.append(f"  📊 Изменение позиций: {chg}")
        if dnf:
            lines.append(f"  ❌ <b>СХОД!</b> Причина: {status}  ·  Кругов: {laps}")
        else:
            lines.append(f"  ✅ Финишировал  ·  Кругов: {laps}")
            if gap_to_win and winner:
                lines.append(f"  📏 До победителя ({winner['Driver']['familyName']}): +{gap_to_win}")
        lines.append(f"  💎 Очков: +{pts}  ·  Итого: {total}")
        if fl_data:
            fl_t = fl_data.get("Time",{}).get("time","")
            fl_r = fl_data.get("rank","")
            if fl_t:
                icon = "⚡" if str(fl_r)=="1" else "🕐"
                extra = " — <b>БЫСТРЕЙШИЙ КРУГ!</b>" if str(fl_r)=="1" else f" (ранг {fl_r})"
                lines.append(f"  {icon} Быстрый круг: <code>{fl_t}</code>{extra}")

        # Хуев в жопе (последняя гонка)
        n_race      = len(race_results)
        ahead_race  = pos - 1
        lines.append(f"  🍆 <b>Хуев в жопе: {ahead_race} из {n_race}</b>")

        # Контекст выступления
        lines.append("")
        if dnf:
            lines.append("  📌 <i>Сход — очки потеряны</i>")
        elif pos == 1:
            lines.append("  📌 <i>Победа! 🎉</i>")
        elif pos <= 3:
            lines.append("  📌 <i>Подиум — отличный результат</i>")
        elif pos <= 10:
            lines.append("  📌 <i>Финиш в очках</i>")
        else:
            lines.append("  📌 <i>За пределами очков</i>")

    lines.append("")

    # ── ПОСЛЕДНЯЯ КВАЛИФИКАЦИЯ ────────────────────────────────────────────────
    sq = next((r for r in q_results if r["Driver"]["familyName"]=="Stroll"), None)
    if sq and qname:
        qp   = int(sq.get("position",0) or 0)
        q1t  = sq.get("Q1","—"); q2t = sq.get("Q2","—"); q3t = sq.get("Q3","—")
        best = q3t if q3t!="—" else (q2t if q2t!="—" else q1t)
        reached = "Q3 🟢" if q3t!="—" else ("Q2 🟡" if q2t!="—" else "Q1 🔴")

        # Сравниваем с напарником Альборном/напарником Aston Martin
        teammate = next((r for r in q_results
                         if r["Constructor"]["name"] == sq["Constructor"]["name"]
                         and r["Driver"]["familyName"] != "Stroll"), None)
        tm_str = ""
        if teammate:
            tm_pos = int(teammate.get("position",0) or 0)
            tm_last= teammate["Driver"]["familyName"]
            tm_best= teammate.get("Q3") or teammate.get("Q2") or teammate.get("Q1") or "—"
            diff   = tm_pos - qp   # >0 = Стролл лучше
            if diff > 0:   tm_str = f"  ·  лучше {tm_last} на {diff} поз. ✅"
            elif diff < 0: tm_str = f"  ·  хуже {tm_last} на {-diff} поз. ⚠️"
            else:          tm_str = f"  ·  наравне с {tm_last}"

        lines.append(f"🔥 <b>Последняя квали: {qname}</b>")
        lines.append(f"  📍 Позиция: <b>P{qp}</b>  ·  Дошёл до: <b>{reached}</b>{tm_str}")
        lines.append(f"  ⏱ Q1: <code>{q1t}</code>  ·  Q2: <code>{q2t}</code>  ·  Q3: <code>{q3t}</code>")
        lines.append(f"  🏆 Лучшее: <code>{best}</code>")

    return "\n".join(lines)

async def fmt_cur_weekend(w):
    lines=["🏁 <b>Текущий / ближайший гоночный уикенд</b>\n",fmt_card(w, show_done=True)]
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
    city    = sess["city"]
    msk_dt  = _msk(sess["start_utc"])
    loc     = sess["start_utc"].astimezone(local_tz(city))
    is_race = "гонка" in sess["summary"].lower()

    lines = [
        f"{sess['flag']} <b>Ближайшее событие</b>",
        "",
        f"{sess['emoji']} <b>{sess['summary']}</b>",
        f"📍 {sess['country']}, {city}",
        f"🕐 <b>{msk_dt.strftime('%H:%M')} МСК</b>  ·  {loc.strftime('%H:%M')} местного",
        f"📅 {msk_dt.day} {MG[msk_dt.month]}",
        "",
    ]
    wdata = await get_weather(city, target_dt=sess["start_utc"])
    lines.append(fmt_weather_block(wdata, target_dt=sess["start_utc"]))

    if is_race:
        _, _qdate, qr = await last_quali()
        if qr:
            lines.append("\n🏁 <b>Стартовая решётка:</b>")
            for q in qr:
                d    = q["Driver"]
                drv  = fmt_driver_jolpica(d, q["Constructor"]["name"])
                best = q.get("Q3") or q.get("Q2") or q.get("Q1") or "—"
                lines.append(f"  <b>P{q['position']}</b>  {drv}  <code>{best}</code>")

    return "\n".join(lines)

async def fmt_standings():
    stds = await driver_standings()
    if not stds:
        return "❌ Данные чемпионата недоступны — результаты гонок ещё не опубликованы"
    lines = ["🏆 <b>Чемпионат гонщиков 2026</b>\n"]
    medals = {1:"🥇", 2:"🥈", 3:"🥉"}
    for i, s in enumerate(stds[:22], 1):
        try:
            d    = s["Driver"]
            team = (s.get("Constructors") or [{}])[0].get("name","—")
            # Jolpica использует "positionText" или "position" — берём что есть
            pos_raw = s.get("position") or s.get("positionText") or str(i)
            try:
                pos = int(pos_raw)
            except (ValueError, TypeError):
                pos = i
            pts  = s.get("points", "0")
            wins = s.get("wins", "0")
            flag = driver_emoji(d.get("familyName",""))
            name = f"{d.get('givenName','')} {d.get('familyName','')}".strip()
            medal = medals.get(pos, "  ")
            w_str = f"  🏆 {wins}п" if wins and wins not in ("0","") else ""
            lines.append(
                f"  {pos:>2}. <b>{pts} очк.</b>  {medal} {flag} {name}\n"
                f"        ({team}){w_str}"
            )
        except Exception as e:
            log.warning("fmt_standings row %d: %s", i, e)
            continue
    return "\n".join(lines)

async def fmt_quali(w: dict = None):
    rname, qdate, results = await last_quali()
    if not results:
        return "📭 Результаты квалификации ещё не опубликованы — сессия не прошла или данные обновляются"
    # Проверяем что результаты относятся к текущему уикенду
    if w and qdate:
        try:
            result_dt = datetime.strptime(qdate, "%Y-%m-%d").replace(tzinfo=pytz.utc)
            w_start   = w["start_utc"] - timedelta(days=1)
            w_end     = w["end_utc"]   + timedelta(days=3)
            if not (w_start <= result_dt <= w_end):
                return (f"⏳ Квалификация {w.get('gp_name','')} ещё не прошла\n"
                        f"или результаты ещё не опубликованы Jolpica.\n\n"
                        f"Последние доступные данные: <b>{rname}</b>")
        except Exception:
            pass
    lines=[f"🔥 <b>Квалификация — {rname}</b>\n"]
    q3=[r for r in results if r.get("Q3")]
    q2=[r for r in results if r.get("Q2") and not r.get("Q3")]
    q1=[r for r in results if not r.get("Q2")]
    if q3:
        lines.append("✅ <b>Q3:</b>")
        for r in q3:
            d   = r["Driver"]
            drv = fmt_driver_jolpica(d, r["Constructor"]["name"])
            lines.append(f"  {r['position']:>2}. {drv}  <code>{r['Q3']}</code>")
    if q2:
        lines.append("\n⚠️ <b>Выбыли в Q2:</b>")
        for r in q2:
            d   = r["Driver"]
            drv = fmt_driver_jolpica(d, r["Constructor"]["name"], bold=False)
            lines.append(f"  {r['position']:>2}. {drv}  <code>{r['Q2']}</code>")
    if q1:
        lines.append("\n❌ <b>Выбыли в Q1:</b>")
        for r in q1:
            d   = r["Driver"]
            drv = fmt_driver_jolpica(d, r["Constructor"]["name"], bold=False)
            lines.append(f"  {r['position']:>2}. {drv}  <code>{r.get('Q1','—')}</code>")

    # ── Аналитика Леклера ────────────────────────────────────────────────────
    lec = next((r for r in results if r["Driver"]["familyName"]=="Leclerc"), None)
    if lec:
        d   = lec["Driver"]
        pos = int(lec["position"])
        q1t = lec.get("Q1","—"); q2t = lec.get("Q2","—"); q3t = lec.get("Q3","—")
        # Лучшее время
        best_t = q3t if q3t != "—" else (q2t if q2t != "—" else q1t)
        # Сравниваем с поулом (P1 Q3)
        pole   = next((r for r in q3 if r["position"]=="1"), None)
        gap    = ""
        if pole and q3t != "—" and pole["Q3"] != q3t:
            gap = f"  · отставание от поула: <code>{pole['Q3']}</code>"

        # Достиг ли Q3?
        reached_q3 = bool(q3t and q3t != "—")
        reached_q2 = bool(q2t and q2t != "—")

        stage = "Q3 🟢" if reached_q3 else ("Q2 🟡" if reached_q2 else "Q1 🔴")

        lines.append(f"\n\n🔴 <b>Шарль Леклер (Ferrari) — детальный разбор</b>")
        lines.append(f"  📍 Стартовая позиция: <b>P{pos}</b>  ·  Дошёл до: <b>{stage}</b>")
        lines.append(f"  ⏱ Q1: <code>{q1t}</code>  ·  Q2: <code>{q2t}</code>  ·  Q3: <code>{q3t}</code>")
        lines.append(f"  🏆 Лучшее время: <code>{best_t}</code>{gap}")
        if pos == 1:
            lines.append("  🏁 <b>ПОУЛ-ПОЗИЦИЯ!</b> 🎉")
        elif pos <= 3:
            lines.append(f"  ✅ Отличная квалификация — топ-3")
        elif pos <= 5:
            lines.append(f"  👍 Хорошая позиция для гонки")
        elif pos > 10:
            lines.append(f"  ⚠️ Выбыл в {'Q1' if not reached_q2 else 'Q2'} — трудный старт гонки")
    return "\n".join(lines)

async def fmt_race(w: dict = None):
    rname, rdate, results = await last_race()
    if not results:
        return "📭 Результаты гонки ещё не опубликованы — гонка не прошла или данные обновляются"
    # Проверяем что результаты относятся к текущему уикенду
    if w and rdate:
        try:
            result_dt = datetime.strptime(rdate, "%Y-%m-%d").replace(tzinfo=pytz.utc)
            w_start   = w["start_utc"] - timedelta(days=1)
            w_end     = w["end_utc"]   + timedelta(days=3)
            if not (w_start <= result_dt <= w_end):
                return (f"⏳ Гонка {w.get('gp_name','')} ещё не прошла\n"
                        f"или результаты ещё не опубликованы.\n\n"
                        f"Последние доступные данные: <b>{rname}</b>")
        except Exception:
            pass
    stds=await driver_standings(); pm={s["Driver"]["driverId"]:s["points"] for s in stds}
    _, _, qresults = await last_quali()
    qpos = {r["Driver"]["driverId"]: int(r["position"]) for r in qresults} if qresults else {}

    medals = {1:"🥇",2:"🥈",3:"🥉"}
    lines = [f"🏁 <b>Гонка — {rname}</b>\n"]

    # Подиум
    lines.append("<b>Подиум:</b>")
    for r in results[:3]:
        d   = r["Driver"]
        pos = int(r["position"])
        drv = fmt_driver_jolpica(d, r["Constructor"]["name"])
        lines.append(f"  {medals[pos]} {drv} +{r.get('points','0')} очк.")

    # Все 22 гонщика
    lines.append("\n<b>Итоговая таблица:</b>")
    finished = [r for r in results if r.get("status","") in ("Finished","+1 Lap","+2 Laps","+3 Laps","+4 Laps","+5 Laps")]
    dnf_list = [r for r in results if r not in finished]
    for r in finished + dnf_list:
        d      = r["Driver"]
        pos    = int(r.get("position",99) or 99)
        total  = pm.get(d["driverId"],"—")
        t_raw  = r.get("Time",{}).get("time","") if pos > 1 else "победитель"
        status = r.get("status","Finished")
        if status not in ("Finished","+1 Lap","+2 Laps","+3 Laps","+4 Laps","+5 Laps") and pos > 1:
            gap = f"❌ Сход ({status})"
        elif pos == 1:
            gap = t_raw or "победитель"
        else:
            gap = f"+{t_raw}" if t_raw else ""
        lines.append(fmt_result_row(pos, d, r["Constructor"]["name"],
                                    time_or_gap=gap,
                                    points=r.get("points","0"),
                                    total_pts=str(total)))

    # Следующий уикенд
    wks=build_weekends(); now=datetime.now(tz=pytz.utc)
    nxt=next((w for w in wks if w["end_utc"]>now),None)
    if nxt:
        rs=next((s for s in nxt["sessions"] if "гонка" in s["summary"].lower()),None)
        if rs:
            lines.append(f"\n📍 Следующая гонка: {nxt['flag']} <b>{nxt['gp_name']}</b>")
            lines.append(f"  🕐 {dtstr(rs['start_utc'])}")

    # ── Детальная аналитика Леклера ──────────────────────────────────────────
    lec = next((r for r in results if r["Driver"]["familyName"]=="Leclerc"), None)
    if lec:
        d      = lec["Driver"]
        pos    = int(lec["position"])
        pts    = lec.get("points","0")
        total  = pm.get(d["driverId"],"—")
        status = lec.get("status","Finished")
        laps   = int(lec.get("laps","0") or 0)
        grid   = int(lec.get("grid","0") or 0)   # стартовая позиция
        q_pos  = qpos.get(d["driverId"], grid)    # позиция квали

        # Быстрый круг
        fl_data = lec.get("FastestLap")
        fl_str  = ""
        fl_rank = ""
        if fl_data:
            fl_str  = fl_data.get("Time",{}).get("time","")
            fl_rank = fl_data.get("rank","")

        # Изменение позиций: квали → финиш
        if q_pos and pos:
            delta = q_pos - pos   # положительное = поднялся
            if delta > 0:
                pos_change = f"+{delta} ↑ ({q_pos}→{pos})"
            elif delta < 0:
                pos_change = f"{delta} ↓ ({q_pos}→{pos})"
            else:
                pos_change = f"без изменений (P{pos})"
        else:
            pos_change = f"P{pos}"

        # Финишировал или нет
        dnf = status not in ("Finished", "+1 Lap", "+2 Laps", "+3 Laps")

        # Победитель
        winner = results[0]
        gap_to_win = lec.get("Time",{}).get("time","") if pos > 1 else ""

        lines.append(f"\n\n🔴 <b>Шарль Леклер (Ferrari) — детальный разбор</b>")
        lines.append(f"")

        # Старт и финиш
        if grid:
            lines.append(f"  🏁 Стартовал с: <b>P{grid}</b> (квали: P{q_pos})")
        lines.append(f"  🏆 Финишировал: <b>P{pos}</b>  ·  Изменение: {pos_change}")

        if dnf:
            lines.append(f"  ❌ <b>Сход!</b> Причина: {status}  ·  Пройдено кругов: {laps}")
        else:
            lines.append(f"  ✅ Финишировал штатно  ·  Кругов: {laps}")
            if gap_to_win:
                w_d = winner["Driver"]
                lines.append(f"  📏 Отставание от победителя ({w_d['familyName']}): +{gap_to_win}")

        # Очки
        lines.append(f"  💎 Очков за гонку: <b>+{pts}</b>  ·  Итого в сезоне: <b>{total}</b>")

        # Быстрый круг
        if fl_str:
            is_fastest = str(fl_rank) == "1"
            fl_icon = "⚡" if is_fastest else "🕐"
            extra = " — <b>БЫСТРЕЙШИЙ КРУГ!</b>" if is_fastest else f" (ранг {fl_rank})"
            lines.append(f"  {fl_icon} Лучший круг: <code>{fl_str}</code>{extra}")

        # Оценка выступления
        lines.append("")
        if dnf:
            lines.append(f"  📊 <i>Сход не позволил бороться — важно посмотреть на темп до схода</i>")
        elif pos == 1:
            lines.append(f"  📊 <i>Победа! Идеальный уикенд для Шарля и Ferrari 🎉</i>")
        elif pos <= 3:
            lines.append(f"  📊 <i>Подиум — сильное выступление</i>")
        elif pos <= 6:
            if delta := (q_pos - pos):
                if delta > 0: lines.append(f"  📊 <i>Хорошая гонка — отыграл {delta} позиций</i>")
                else: lines.append(f"  📊 <i>Потерял {-delta} позиций по ходу гонки</i>")
            else:
                lines.append(f"  📊 <i>Ровная гонка — удержал стартовую позицию</i>")
        else:
            lines.append(f"  📊 <i>Тяжёлый уикенд для Ferrari</i>")

    return "\n".join(lines)

async def fmt_cur_weekend_with_results(w: dict) -> tuple:
    """Карточка текущего уикенда + клавиатура с результатами."""
    lines = ["🏁 <b>Текущий / ближайший гоночный уикенд</b>\n", fmt_card(w, show_done=True)]
    stds  = await driver_standings()
    if stds:
        lines.append("\n\n🏆 <b>Чемпионат гонщиков 2026</b>")
        for s in stds[:10]:
            d    = s["Driver"]
            team = (s.get("Constructors") or [{}])[0].get("name","—")
            drv  = fmt_driver_jolpica(d, team)
            lines.append(f"  {int(s['position']):>2}. {drv} · {s['points']} очк.")
    return "\n".join(lines)

async def fmt_past_weekend(w: dict, rnd: int) -> str:
    """Результаты прошедшего уикенда: гонка + чемпионат."""
    rname, results = await race_by_round(2026, rnd)
    stds           = await driver_standings()
    pts_map        = {s["Driver"]["driverId"]: s["points"] for s in stds}

    lines = [
        f"{w['flag']} <b>{w['gp_name'].upper()}</b>",
        f"<i>📍 {w['country']}, {w['city']}</i>",
        "",
    ]

    medals = {1:"🥇",2:"🥈",3:"🥉"}
    if results:
        lines.append("<b>Подиум:</b>")
        for r in results[:3]:
            d   = r["Driver"]
            pos = int(r["position"])
            drv = fmt_driver_jolpica(d, r["Constructor"]["name"])
            lines.append(f"  {medals[pos]} {drv} +{r.get('points','0')} очк.")
        lines.append("")
        lines.append("<b>Итоговая таблица:</b>")
        finished_p = [r for r in results if r.get("status","") in ("Finished","+1 Lap","+2 Laps","+3 Laps","+4 Laps","+5 Laps")]
        dnf_p      = [r for r in results if r not in finished_p]
        for r in finished_p + dnf_p:
            d      = r["Driver"]
            total  = pts_map.get(d["driverId"], "—")
            pos    = int(r.get("position",99) or 99)
            t_raw  = r.get("Time",{}).get("time","") if pos > 1 else "победитель"
            status = r.get("status","Finished")
            if status not in ("Finished","+1 Lap","+2 Laps","+3 Laps","+4 Laps","+5 Laps") and pos > 1:
                gap = f"❌ Сход ({status})"
            elif pos == 1:
                gap = t_raw or "победитель"
            else:
                gap = f"+{t_raw}" if t_raw else ""
            lines.append(fmt_result_row(pos, d, r["Constructor"]["name"],
                                        time_or_gap=gap,
                                        points=r.get("points","0"),
                                        total_pts=str(total)))
    else:
        lines.append("📭 Результаты гонки ещё не опубликованы")

    if stds:
        lines.append("\n🏆 <b>Чемпионат после этапа (топ-10):</b>")
        for s in stds[:10]:
            d    = s["Driver"]
            team = (s.get("Constructors") or [{}])[0].get("name","—")
            drv  = fmt_driver_jolpica(d, team)
            pos_raw = s.get("position") or s.get("positionText","?")
            lines.append(f"  {pos_raw}. {drv} — {s['points']} очк.")

    return "\n".join(lines)

async def fmt_past_quali(w: dict, rnd: int) -> str:
    """Результаты квалификации прошедшего уикенда."""
    rname, results = await quali_by_round(2026, rnd)
    if not results:
        return "📭 Результаты квалификации ещё не опубликованы"

    lines = [
        f"{w['flag']} <b>{w['gp_name']} — Квалификация</b>",
        "",
    ]
    q3 = [r for r in results if r.get("Q3")]
    q2 = [r for r in results if r.get("Q2") and not r.get("Q3")]
    q1 = [r for r in results if not r.get("Q2")]

    if q3:
        lines.append("✅ <b>Q3:</b>")
        for r in q3:
            d   = r["Driver"]
            drv = fmt_driver_jolpica(d, r["Constructor"]["name"])
            lines.append(f"  {r['position']:>2}. {drv}  <code>{r['Q3']}</code>")
    if q2:
        lines.append("\n⚠️ <b>Выбыли в Q2:</b>")
        for r in q2:
            d   = r["Driver"]
            drv = fmt_driver_jolpica(d, r["Constructor"]["name"], bold=False)
            lines.append(f"  {r['position']:>2}. {drv}  <code>{r['Q2']}</code>")
    if q1:
        lines.append("\n❌ <b>Выбыли в Q1:</b>")
        for r in q1:
            d   = r["Driver"]
            drv = fmt_driver_jolpica(d, r["Constructor"]["name"], bold=False)
            lines.append(f"  {r['position']:>2}. {drv}  <code>{r.get('Q1','—')}</code>")
    return "\n".join(lines)

# ── LIVE MONITOR ──────────────────────────────────────────────────────────────
class LiveMonitor:
    POLL_INTERVAL = 7   # сек между опросами

    def __init__(self, sk: int, gp: str, city: str, sess_type: str = "race",
                 mv_path: str = None, year: int = 2026):
        self.key       = sk
        self.gp        = gp
        self.city      = city
        self.sess_type = sess_type
        self.mv_path   = mv_path   # путь для Multiviewer, например "1/Q"
        self.year      = year
        self.running   = False
        self.drivers:      dict  = {}
        self.seen_rc:      set   = set()
        self.seen_pit:     set   = set()
        self.best_lap:     Optional[float] = None
        self.form_sent:    bool  = False
        self.start_sent:   bool  = False
        self.finish_sent:  bool  = False
        self.podium_sent:  set   = set()
        self._post_race_task = None

    # ── запуск ───────────────────────────────────────────────────────────────
    async def run(self):
        self.running = True
        log.info("🔴 LiveMonitor start sk=%s  %s  type=%s", self.key, self.gp, self.sess_type)

        # Параллельно грузим гонщиков + историю RC + питы + круги
        (self.drivers,
         history_rc,
         hist_pits,
         hist_laps) = await asyncio.gather(
            f1_drivers(self.key),
            f1_rc(self.key, self.mv_path, self.year),
            f1_pit(self.key),
            f1_laps(self.key),
            return_exceptions=True
        )
        if isinstance(self.drivers, Exception): self.drivers = {}
        if isinstance(history_rc,   Exception): history_rc   = []
        if isinstance(hist_pits,    Exception): hist_pits    = []
        if isinstance(hist_laps,    Exception): hist_laps    = []

        # Глотаем историю — не отправляем
        for m in history_rc:
            self.seen_rc.add(f"{m.get('date','')}{m.get('message','')}")
        for p in hist_pits:
            self.seen_pit.add(f"{p.get('driver_number')}_{p.get('lap_number')}_{p.get('pit_duration')}")
        valid_laps = [l for l in hist_laps if l.get("lap_duration","") and l["lap_duration"] > 0]
        if valid_laps:
            self.best_lap = min(l["lap_duration"] for l in valid_laps)

        log.info("История: %d RC, %d pit, best=%.3fs, %d гонщиков",
                 len(self.seen_rc), len(self.seen_pit),
                 self.best_lap or 0, len(self.drivers))

        while self.running:
            try:
                await self._poll()
            except Exception as e:
                log.error("LiveMonitor poll: %s", e)
            await asyncio.sleep(self.POLL_INTERVAL)

    def stop(self):
        self.running = False
        log.info("LiveMonitor stopped — %s", self.gp)

    def _name(self, n) -> str:
        d = self.drivers.get(n, {})
        return d.get("full_name") or d.get("broadcast_name") or f"#{n}"
    def _team(self, n) -> str:
        return self.drivers.get(n, {}).get("team_name") or ""
    def _flag(self, n) -> str:
        full = self._name(n)
        last = full.split()[-1] if full else ""
        return driver_emoji(last)

    async def _bcast(self, text: str):
        await broadcast(text)

    async def _poll(self):
        """Параллельный опрос всех источников."""
        if self.sess_type in ("race", "sprint"):
            await asyncio.gather(
                self._rc(),
                self._pitstop(),
                self._fl(),
                self._positions(),
                return_exceptions=True
            )
        elif self.sess_type in ("quali", "sprint_quali"):
            await asyncio.gather(
                self._rc(),
                self._fl(),
                return_exceptions=True
            )
        else:  # practice
            await self._rc()

    # ── Race Control ─────────────────────────────────────────────────────────
    async def _rc(self):
        msgs = await f1_rc(self.key, self.mv_path, self.year)
        for m in msgs:
            uid = f"{m.get('date','')}{m.get('message','')}"
            if uid in self.seen_rc:
                continue
            self.seen_rc.add(uid)
            await self._handle_rc(m)

    async def _handle_rc(self, m: dict):
        text = m.get("message", "")
        flag = (m.get("flag") or "").upper()
        cat  = (m.get("category") or "").upper()
        up   = text.upper()

        if ("FORMATION LAP" in up or "FORMATION" in up) and not self.form_sent:
            self.form_sent = True
            label = "Прогревочный круг" if self.sess_type == "race" else "Сессия начинается"
            await self._bcast(f"🏎️ <b>{label}!</b>\n\n<b>{self.gp}</b>")

        elif (flag == "GREEN" or "GREEN LIGHT" in up or "RACE START" in up)                 and not self.start_sent and self.sess_type in ("race", "sprint"):
            self.start_sent = True
            label = "ГОНКА" if self.sess_type == "race" else "СПРИНТ"
            w = await get_weather(self.city)
            await self._bcast(f"🚦 <b>{label} СТАРТОВАЛ!</b>\n\n<b>{self.gp}</b>\n\n{fmt_weather(w)}")

        elif "SAFETY CAR" in up and "VIRTUAL" not in up and "MEDICAL" not in up:
            if "DEPLOYED" in up or flag == "SC":
                await self._bcast(f"🚗 <b>Safety Car!</b>\n{text}")
            elif "WITHDRAWN" in up or "IN THIS LAP" in up:
                await self._bcast("🚗 <b>Safety Car возвращается в боксы</b>")

        elif flag == "VSC" or "VIRTUAL SAFETY CAR" in up:
            if "DEPLOYED" in up or flag == "VSC":
                await self._bcast("🔶 <b>Virtual Safety Car (VSC)!</b>")
            elif "ENDING" in up or "RESUMED" in up:
                await self._bcast("🔶 <b>VSC заканчивается — рестарт!</b>")

        elif flag == "YELLOW":
            scope = m.get("scope", "")
            await self._bcast(f"🟡 <b>Жёлтый флаг</b>{' · Сектор ' + scope if scope else ''}\n{text}")

        elif flag == "RED":
            await self._bcast(f"🔴 <b>КРАСНЫЙ ФЛАГ!</b>\n\n{self.gp}\n{text}")

        elif "PENALTY" in up or "SANCTION" in up or "DRIVE THROUGH" in up or "STOP AND GO" in up:
            await self._bcast(f"⚖️ <b>Штраф</b>\n{text}")

        elif (flag == "CHEQUERED" or "CHEQUERED" in up or "CHECKERED" in up)                 and not self.finish_sent:
            self.finish_sent = True
            labels = {"race":"Гонка","sprint":"Спринт","quali":"Квалификация",
                      "sprint_quali":"Квалификация к спринту","practice":"Практика"}
            label = labels.get(self.sess_type, "Сессия")
            await self._bcast(f"🏁 <b>{label} завершена!</b>\n\n<b>{self.gp}</b>")
            if self.sess_type in ("race", "sprint"):
                asyncio.ensure_future(self._post_race_results())
            await asyncio.sleep(180)
            self.stop()

    # ── Пит-стопы ────────────────────────────────────────────────────────────
    async def _pitstop(self):
        pits = await f1_pit(self.key)
        new_pits = []
        for p in pits:
            dur = p.get("pit_duration")
            if not dur:
                continue
            pid = f"{p.get('driver_number')}_{p.get('lap_number')}_{dur}"
            if pid not in self.seen_pit:
                self.seen_pit.add(pid)
                new_pits.append(p)
        # Отправляем батчем (не блокируем цикл)
        for p in new_pits:
            dn = p.get("driver_number")
            name = self._name(dn); flag = self._flag(dn); team = self._team(dn)
            await self._bcast(
                f"🔧 <b>Пит-стоп!</b>\n"
                f"{flag} <b>{name}</b> ({team})\n"
                f"📌 Круг {p.get('lap_number','?')}  ·  "
                f"⏱ {float(dur):.1f} с"
            )

    # ── Быстрый круг ─────────────────────────────────────────────────────────
    async def _fl(self):
        laps = await f1_laps(self.key)
        if not laps:
            return
        fastest = min(laps, key=lambda l: l["lap_duration"])
        dur = fastest["lap_duration"]
        if self.best_lap is not None and dur >= self.best_lap:
            return
        self.best_lap = dur
        dn   = fastest["driver_number"]
        mins = int(dur // 60); secs = dur % 60
        name = self._name(dn); flag = self._flag(dn); team = self._team(dn)
        await self._bcast(
            f"⚡ <b>Новый быстрый круг!</b>\n"
            f"{flag} <b>{name}</b> ({team})\n"
            f"⏱ <b>{mins}:{secs:06.3f}</b>  ·  Круг {fastest.get('lap_number','?')}"
        )

    # ── Финиш P1/P2/P3 ───────────────────────────────────────────────────────
    async def _positions(self):
        if len(self.podium_sent) >= 3:
            return
        pos_data = await f1_positions(self.key)
        if not pos_data:
            return
        latest: dict = {}
        for p in pos_data:
            latest[p.get("driver_number")] = p
        for dn, p in latest.items():
            position = p.get("position")
            if position in (1, 2, 3) and f"P{position}" not in self.podium_sent:
                lap = p.get("lap_number", 0) or 0
                if lap >= 45:
                    medals = {1:"🥇", 2:"🥈", 3:"🥉"}
                    self.podium_sent.add(f"P{position}")
                    name = self._name(dn); flag = self._flag(dn); team = self._team(dn)
                    await self._bcast(
                        f"{medals[position]} <b>P{position} — {name}!</b>\n"
                        f"{flag} ({team})  ·  Круг {lap}"
                    )

    # ── Авто-результаты после финиша ─────────────────────────────────────────
    async def _post_race_results(self):
        """
        Сразу после гонки: итоговые позиции из OpenF1.
        Через 10–40 мин (когда Jolpica обновится): официальные результаты.
        """
        await asyncio.sleep(90)   # ждём финальные позиции OpenF1

        # Шаг 1: быстрые результаты из OpenF1 positions
        pos_data = await f1_positions(self.key)
        if pos_data:
            latest: dict = {}
            for p in pos_data:
                dn = p.get("driver_number")
                if dn not in latest or p.get("lap_number",0) > latest[dn].get("lap_number",0):
                    latest[dn] = p
            sorted_pos = sorted(latest.values(), key=lambda x: x.get("position", 99))
            medals = {1:"🥇",2:"🥈",3:"🥉"}
            lines  = [f"🏁 <b>Итоги гонки — {self.gp}</b>\n", "<b>Топ-10:</b>"]
            for p in sorted_pos[:10]:
                dn   = p.get("driver_number")
                pos  = p.get("position", "?")
                name = self._name(dn); flag = self._flag(dn); team = self._team(dn)
                medal = medals.get(pos, f"{pos}.")
                lines.append(f"  {medal} {flag} {name} ({team})")
            lines.append("\n<i>Официальные результаты появятся позже в разделе «Итоги»</i>")
            await self._bcast("\n".join(lines))

        # Шаг 2: ждём Jolpica (проверяем каждые 10 мин, до 1 часа)
        for attempt in range(6):
            await asyncio.sleep(600)
            rname, _, results = await last_race()
            if results:
                stds = await driver_standings()
                pts  = {s["Driver"]["driverId"]: s["points"] for s in stds}
                lines = [f"📊 <b>Официальные результаты — {rname}</b>\n"]
                for r in results[:10]:
                    d    = r["Driver"]
                    last = d["familyName"]
                    flag = driver_emoji(last)
                    pos  = int(r["position"])
                    medal = medals.get(pos, f"{pos}.")
                    total = pts.get(d["driverId"], "—")
                    drv = fmt_driver_jolpica(d, r["Constructor"]["name"])
                lines.append(f"  {medal} {drv} +{r.get('points','0')} → {total} очк.")
                await self._bcast("\n".join(lines))
                log.info("Официальные результаты отправлены после %d мин", (attempt+1)*10)
                return
        log.warning("Jolpica так и не вернул результаты через час после гонки")


_monitor: Optional[LiveMonitor] = None

# Активные фоновые задачи загрузки: message_id → Task
_loading_tasks: dict[int, asyncio.Task] = {}

# Маппинг типов сессий на ключевые слова OpenF1
_SESS_KEYWORDS = {
    "race":           ("race",),
    "sprint":         ("sprint shootout", "sprint race", "sprint"),
    "sprint_quali":   ("sprint qualifying", "sprint shootout"),
    "quali":          ("qualifying",),
    "practice":       ("practice",),
}

def _sess_type_matches(sname: str, sess_type: str) -> bool:
    sname = sname.lower()
    keywords = _SESS_KEYWORDS.get(sess_type, ())
    return any(k in sname for k in keywords)

def _parse_openf1_dt(s: str) -> Optional[datetime]:
    if not s: return None
    try:
        s = s.rstrip("Z")
        if "+" not in s: s += "+00:00"
        return datetime.fromisoformat(s).astimezone(pytz.utc)
    except Exception:
        return None

async def find_openf1_session(gp_name: str, sess_type: str) -> Optional[dict]:
    """
    Ищет нужную сессию в OpenF1:
    1. latest — если совпадает по типу
    2. Все сессии года → ищем начавшуюся в последние 3 ч с нужным типом
    3. Повторяем 5 раз с паузой 30 с
    """
    now = datetime.now(tz=pytz.utc)

    for attempt in range(5):
        # Попытка А: latest
        sess = await f1_latest_sess()
        if sess:
            sname = (sess.get("session_name") or "").lower()
            if _sess_type_matches(sname, sess_type):
                log.info("OpenF1 latest: sk=%s '%s'", sess["session_key"], sname)
                return sess

        # Попытка Б: все сессии года, ищем по времени + типу
        year = now.year
        sessions = await oget(f"/sessions?year={year}")
        for s in sorted(sessions, key=lambda x: x.get("date_start",""), reverse=True):
            s_start = _parse_openf1_dt(s.get("date_start",""))
            if not s_start: continue
            delta = (now - s_start).total_seconds()
            if not (-300 <= delta <= 10800):  # от -5 мин до +3 ч
                continue
            sname = (s.get("session_name") or "").lower()
            if _sess_type_matches(sname, sess_type):
                log.info("OpenF1 по времени: sk=%s '%s' delta=%.0fs", s["session_key"], sname, delta)
                return s

        log.info("find_openf1_session: попытка %d/5 для %s %s", attempt+1, gp_name, sess_type)
        if attempt < 4:
            await asyncio.sleep(30)

    log.warning("find_openf1_session: не нашли для %s %s", gp_name, sess_type)
    return None

async def start_live(gp: str, city: str, sess_type: str = "race"):
    """Запускает поиск сессии и монитор в фоне — не блокирует event loop."""
    asyncio.create_task(_start_live_bg(gp, city, sess_type))

async def _start_live_bg(gp: str, city: str, sess_type: str):
    global _monitor
    if _monitor and _monitor.running:
        _monitor.stop()
    sess = await find_openf1_session(gp, sess_type)
    if not sess:
        log.warning("start_live: сессия не найдена для %s %s", gp, sess_type)
        return
    year = datetime.now(tz=pytz.utc).year
    rnd  = sess.get("meeting_key", 0)
    abbr_map = {"race":"R","sprint":"S","sprint_quali":"SQ","quali":"Q","practice":"P"}
    mv_path  = f"{rnd}/{abbr_map.get(sess_type,'R')}"
    log.info("start_live: sk=%s gp=%s type=%s mv=%s", sess["session_key"], gp, sess_type, mv_path)
    _monitor = LiveMonitor(sess["session_key"], gp, city, sess_type, mv_path=mv_path, year=year)
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
    try:
        wdata = await asyncio.wait_for(get_weather(city, target_dt=ev["start_utc"]), timeout=7.0)
    except Exception:
        wdata = {"current": {}, "forecast": None}
    lines.append(fmt_weather_block(wdata, target_dt=ev["start_utc"]))
    if "гонка" in ev["summary"].lower() and mins >= 0:
        _,_,qr=await last_quali()
        if qr:
            lines.append("\n🏁 <b>Стартовая решётка:</b>")
            for q in qr:
                d   = q["Driver"]
                drv = fmt_driver_jolpica(d, q["Constructor"]["name"])
                lines.append(f"  <b>P{q['position']}</b>  {drv}")
    text="\n".join(lines)
    await broadcast(text)

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
            summ_lower = s["summary"].lower()
            is_race         = "гонка" in summ_lower and "спринт" not in summ_lower
            is_sprint       = "спринт" in summ_lower and "квалификация" not in summ_lower and "к спринту" not in summ_lower
            is_sprint_quali = "к спринту" in summ_lower or ("спринт" in summ_lower and "квалификация" in summ_lower)
            is_quali        = "квалификация" in summ_lower and not is_sprint_quali
            is_practice     = "свободных" in summ_lower
            if (is_race or is_quali or is_sprint or is_sprint_quali or is_practice) and start>now:
                t_live = start + timedelta(minutes=2)
                if is_race:             stype = "race"
                elif is_sprint:         stype = "sprint"
                elif is_sprint_quali:   stype = "sprint_quali"
                elif is_quali:          stype = "quali"
                else:                   stype = "practice"
                scheduler.add_job(start_live,"date",run_date=t_live,
                                  args=[w["gp_name"],w["city"],stype],
                                  id=f"live_{s['summary']}_{start.isoformat()}",replace_existing=True)
    log.info("Запланировано %d напоминаний, %d уикендов",count,len(wks))

# ── LOADING TASK MANAGER ─────────────────────────────────────────────────────
def cancel_kb(msg_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✖️ Отмена", callback_data=f"cancel:{msg_id}")]
    ])

async def run_with_cancel(q, loading_text: str, coro, reply_kb_fn=None):
    """
    Показывает «загружаю + кнопка Отмена», запускает coro как Task.
    Если задача завершилась — показывает результат.
    Если отменена — ничего (on_cb уже показал меню).
    reply_kb_fn: async callable() → InlineKeyboardMarkup
    """
    msg_id = q.message.message_id
    # Показываем кнопку отмены
    await q.edit_message_text(
        f"⏳ {loading_text}",
        parse_mode="HTML",
        reply_markup=cancel_kb(msg_id)
    )

    async def _work():
        try:
            text = await coro
            kb   = (await reply_kb_fn()) if reply_kb_fn else back_kb()
            await q.edit_message_text(text[:4090], parse_mode="HTML", reply_markup=kb)
        except asyncio.CancelledError:
            pass  # отменено пользователем — on_cb уже отрисовал меню
        except Exception as e:
            log.error("run_with_cancel coro: %s", e)
            try:
                await q.edit_message_text(
                    "⚠️ Не удалось загрузить данные. Попробуй ещё раз.",
                    reply_markup=back_kb()
                )
            except Exception:
                pass
        finally:
            _loading_tasks.pop(msg_id, None)

    task = asyncio.ensure_future(_work())
    _loading_tasks[msg_id] = task

# ── KEYBOARDS ─────────────────────────────────────────────────────────────────
REPLY_KB=ReplyKeyboardMarkup([["🏠 Меню"]],resize_keyboard=True,is_persistent=True)
def _home(): return [InlineKeyboardButton("🏠 Меню",callback_data="home")]

def main_kb(cid, priv):
    rows = []
    if priv:
        subbed  = is_subscribed(cid)
        enabled = subbed and not is_muted(cid)
        if not subbed:   label = "🔔 Уведомления — выкл"
        elif enabled:    label = "✅ Уведомления — вкл"
        else:            label = "☑️ Уведомления — выкл"
        rows.append([InlineKeyboardButton(label, callback_data="notif_toggle")])
    rows += [
        [InlineKeyboardButton("⚡ Ближайшее событие",       callback_data="cal:next_sess")],
        [InlineKeyboardButton("🏁 Этот уикенд",             callback_data="cal:current")],
        [InlineKeyboardButton("⏭ Следующий уикенд",         callback_data="cal:next")],
        [InlineKeyboardButton("🗓 Календарь всех событий",  callback_data="cal:months")],
        [InlineKeyboardButton("🕰 Прошедшие уикенды",       callback_data="cal:past")],
        [InlineKeyboardButton("🏆 Баллы за сезон",          callback_data="res:standings")],
        [InlineKeyboardButton("🟦 Статистика Стролла",       callback_data="res:stroll")],
        [InlineKeyboardButton("✖️ Закрыть",                  callback_data="close")],
    ]
    return InlineKeyboardMarkup(rows)

async def cur_weekend_kb(w: dict = None):
    """Кнопки текущего уикенда с индикатором наличия результатов.
    Сравниваем по дате: результат «свежий» если дата из Jolpica
    попадает в окно уикенда (start_utc-1д … end_utc+2д)."""
    _, qdate, qr = await last_quali()
    _, rdate, rr = await last_race()

    def _result_belongs(date_str: str) -> bool:
        """date_str — 'YYYY-MM-DD' из Jolpica."""
        if not w or not date_str:
            return bool(qr or rr)   # нет уикенда — показываем если есть хоть что-то
        try:
            result_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=pytz.utc)
            w_start   = w["start_utc"] - timedelta(days=1)
            w_end     = w["end_utc"]   + timedelta(days=2)
            return w_start <= result_dt <= w_end
        except Exception:
            return False

    q_dot = "🟢" if (qr and _result_belongs(qdate)) else "⚫️"
    r_dot = "🟢" if (rr and _result_belongs(rdate)) else "⚫️"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🔥 Квалификация — результаты {q_dot}", callback_data="res:quali")],
        [InlineKeyboardButton(f"🏁 Гонка — результаты {r_dot}",        callback_data="res:race")],
        _home(),
    ])

def months_kb(wks):
    seen = OrderedDict()
    for w in wks:
        m = _msk(w["start_utc"]); seen[(m.year, m.month)] = MR[m.month]
    rows = []; row = []
    for (y, mo), name in seen.items():
        row.append(InlineKeyboardButton(name, callback_data=f"cal:month:{y}-{mo:02d}"))
        if len(row) == 3: rows.append(row); row = []
    if row: rows.append(row)
    rows.append(_home()); return InlineKeyboardMarkup(rows)

def month_nav_kb(wks, year, month):
    all_m = sorted({(_msk(w["start_utc"]).year, _msk(w["start_utc"]).month) for w in wks})
    idx   = all_m.index((year, month)) if (year, month) in all_m else 0
    nav   = []
    if idx > 0:
        py,pm = all_m[idx-1]; nav.append(InlineKeyboardButton(f"◀️ {MR[pm][:3]}", callback_data=f"cal:month:{py}-{pm:02d}"))
    if idx < len(all_m)-1:
        ny,nm = all_m[idx+1]; nav.append(InlineKeyboardButton(f"{MR[nm][:3]} ▶️", callback_data=f"cal:month:{ny}-{nm:02d}"))
    rows = [nav] if nav else []
    rows.append([InlineKeyboardButton("📅 Все месяцы", callback_data="cal:months")])
    rows.append(_home()); return InlineKeyboardMarkup(rows)

def past_weekends_kb(wks):
    """Кнопки прошедших уикендов."""
    now  = datetime.now(tz=pytz.utc)
    past = [w for w in wks if w["end_utc"] < now]
    rows = []
    for i, w in enumerate(reversed(past)):  # свежие сверху
        s = _msk(w["start_utc"]); e = _msk(w["end_utc"])
        if s.month == e.month:
            dates = f"{s.day}–{e.day} {MG[s.month]}"
        else:
            dates = f"{s.day} {MG[s.month]}–{e.day} {MG[e.month]}"
        label = f"{w['flag']} {w['country']}, {w['city']}  ·  {dates}"
        rnd   = len(past) - i   # номер раунда
        rows.append([InlineKeyboardButton(label, callback_data=f"cal:past:{rnd}")])
    rows.append(_home())
    return InlineKeyboardMarkup(rows)

async def past_weekend_detail_kb(rnd: int):
    """Клавиатура страницы результатов гонки прошедшего уикенда."""
    _, qr = await quali_by_round(2026, rnd)
    q_dot = "🟢" if qr else "⚫️"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🔥 Квалификация — результаты {q_dot}", callback_data=f"cal:past_quali:{rnd}")],
        [InlineKeyboardButton("◀️ Все прошедшие уикенды", callback_data="cal:past")],
        _home(),
    ])

def past_quali_kb(rnd: int):
    """Клавиатура страницы квалификации прошедшего уикенда."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("◀️ Результаты гонки",        callback_data=f"cal:past:{rnd}")],
        [InlineKeyboardButton("◀️ Все прошедшие уикенды",   callback_data="cal:past")],
        _home(),
    ])

def back_kb(): return InlineKeyboardMarkup([_home()])
def results_kb(): return InlineKeyboardMarkup([
    [InlineKeyboardButton("🔥 Квалификация", callback_data="res:quali"),
     InlineKeyboardButton("🏁 Гонка",         callback_data="res:race")],
    _home(),
])

# ── HELPERS ───────────────────────────────────────────────────────────────────
def _past_weekends():
    now = datetime.now(tz=pytz.utc)
    return [w for w in build_weekends() if w["end_utc"] < now]

def _round_for_past(wks_past: list, rnd: int) -> Optional[dict]:
    """rnd — 1-based, свежие сначала (reversed порядок)."""
    ordered = list(reversed(wks_past))
    if 1 <= rnd <= len(ordered):
        return ordered[rnd - 1]
    return None

# ── HANDLERS ──────────────────────────────────────────────────────────────────
async def cmd_start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat = upd.effective_chat; priv = chat.type == "private"
    subscribe(chat.id, priv)
    desc = (
        "⏰ Напоминания <b>за 30 мин</b> и <b>за 5 мин</b> до каждой сессии\n"
        "🌡 Погода + местное время в напоминаниях\n"
        "🔴 Live-события во время гонки\n"
        "🏆 Результаты и чемпионат\n\n"
        "Нажми <b>🔔 Уведомления — выкл</b> чтобы включить!"
        if priv else
        "Этот чат добавлен — уведомления будут приходить сюда!\n\nИспользуй /start для меню."
    )
    await upd.message.reply_text("🏠", reply_markup=REPLY_KB)
    await upd.message.reply_text(
        f"🏎️ <b>F1 2026 — бот уведомлений</b>\n\n{desc}",
        parse_mode="HTML", reply_markup=main_kb(chat.id, priv)
    )

async def on_menu(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat = upd.effective_chat; priv = chat.type == "private"
    # Удаляем сообщение пользователя «🏠 Меню» (тихо — в группах может не быть прав)
    try:
        await upd.message.delete()
    except Exception:
        pass
    # Отправляем через chat, а не через message (message уже удалён)
    await ctx.bot.send_message(
        chat_id=chat.id,
        text="🏎️ <b>F1 2026 — главное меню</b>",
        parse_mode="HTML",
        reply_markup=main_kb(chat.id, priv)
    )

async def cmd_status(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    d   = _load(); now = datetime.now(tz=MSK)
    live = "✅ активен" if (_monitor and _monitor.running) else "💤 не активен"
    await upd.message.reply_text(
        f"📊 <b>Статус бота</b>\n\n"
        f"👤 Личных: {len(d['users'])}\n"
        f"💬 Чатов: {len(d['chats'])}\n"
        f"🔕 Выключили: {len(d.get('muted',[]))}\n"
        f"⏰ Напоминаний: {len(scheduler.get_jobs())}\n"
        f"🔴 Live: {live}\n"
        f"🕐 {now.strftime('%d.%m.%Y %H:%M')} МСК",
        parse_mode="HTML"
    )

async def on_cb(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q: CallbackQuery = upd.callback_query
    await q.answer()
    data = q.data
    chat = upd.effective_chat
    priv = chat.type == "private"

    # ── Закрыть ───────────────────────────────────────────────────────────────
    if data == "close":
        try: await q.message.delete()
        except: await q.edit_message_reply_markup(reply_markup=None)
        return

    # ── Отмена загрузки ───────────────────────────────────────────────────────
    if data.startswith("cancel:"):
        msg_id = int(data.split(":")[1])
        task = _loading_tasks.pop(msg_id, None)
        if task and not task.done():
            task.cancel()
        await q.edit_message_text(
            "🏎️ <b>F1 2026 — главное меню</b>",
            parse_mode="HTML", reply_markup=main_kb(chat.id, priv)
        )
        return

    # ── Главное меню ──────────────────────────────────────────────────────────
    if data == "home":
        await q.edit_message_text(
            "🏎️ <b>F1 2026 — главное меню</b>",
            parse_mode="HTML", reply_markup=main_kb(chat.id, priv)
        ); return

    # ── Уведомления ───────────────────────────────────────────────────────────
    if data == "notif_toggle" and priv:
        enabled = toggle_notif(chat.id, True)
        await q.answer("✅ Уведомления включены!" if enabled else "☑️ Уведомления выключены", show_alert=True)
        await q.edit_message_reply_markup(reply_markup=main_kb(chat.id, True)); return

    # ── Ближайшее событие ─────────────────────────────────────────────────────
    if data == "cal:next_sess":
        s = nxt_session(build_weekends())
        if not s:
            await q.edit_message_text("😔 Нет предстоящих событий.", reply_markup=back_kb()); return
        await run_with_cancel(q, "Загружаю погоду...", fmt_next_sess(s)); return

    # ── Этот уикенд (с кнопками результатов) ─────────────────────────────────
    if data == "cal:current":
        wks = build_weekends(); w = cur_weekend(wks) or nxt_weekend(wks)
        if not w:
            await q.edit_message_text("😔 Нет данных.", reply_markup=back_kb()); return
        _w = w
        await run_with_cancel(q, "Загружаю...", fmt_cur_weekend_with_results(_w),
                              reply_kb_fn=lambda: cur_weekend_kb(_w)); return

    # ── Следующий уикенд ──────────────────────────────────────────────────────
    if data == "cal:next":
        w = nxt_weekend(build_weekends())
        if not w:
            await q.edit_message_text("😔 Нет предстоящих уикендов.", reply_markup=back_kb()); return
        await q.edit_message_text(fmt_card(w, show_done=False), parse_mode="HTML", reply_markup=back_kb()); return

    # ── Календарь: выбор месяца ────────────────────────────────────────────────
    if data == "cal:months":
        wks = build_weekends()
        await q.edit_message_text(
            "📅 <b>Выбери месяц:</b>", parse_mode="HTML", reply_markup=months_kb(wks)
        ); return

    if data.startswith("cal:month:"):
        ym    = data.split(":", 2)[2]
        year  = int(ym.split("-")[0]); month = int(ym.split("-")[1])
        wks   = build_weekends()
        text  = fmt_month(wks, year, month)
        await q.edit_message_text(text, parse_mode="HTML", reply_markup=month_nav_kb(wks, year, month)); return

    # ── Прошедшие уикенды ─────────────────────────────────────────────────────
    if data == "cal:past":
        past = _past_weekends()
        if not past:
            await q.edit_message_text("😔 Ещё не прошло ни одного уикенда.", reply_markup=back_kb()); return
        await q.edit_message_text(
            "🕰 <b>Прошедшие уикенды:</b>", parse_mode="HTML",
            reply_markup=past_weekends_kb(past)
        ); return

    if data.startswith("cal:past:") and not data.startswith("cal:past_quali:"):
        rnd  = int(data.split(":")[-1])
        past = _past_weekends()
        w    = _round_for_past(past, rnd)
        if not w:
            await q.edit_message_text("😔 Уикенд не найден.", reply_markup=back_kb()); return
        _rnd = rnd
        await run_with_cancel(q, "Загружаю результаты...", fmt_past_weekend(w, _rnd),
                              reply_kb_fn=lambda: past_weekend_detail_kb(_rnd)); return

    if data.startswith("cal:past_quali:"):
        rnd  = int(data.split(":")[-1])
        past = _past_weekends()
        w    = _round_for_past(past, rnd)
        if not w:
            await q.edit_message_text("😔 Уикенд не найден.", reply_markup=back_kb()); return
        _rnd = rnd
        async def _pq_kb(): return past_quali_kb(_rnd)
        await run_with_cancel(q, "Загружаю квалификацию...", fmt_past_quali(w, _rnd),
                              reply_kb_fn=_pq_kb); return

    # ── Чемпионат ─────────────────────────────────────────────────────────────
    if data == "res:standings":
        await run_with_cancel(q, "Загружаю баллы за сезон...", fmt_standings()); return

    if data == "res:stroll":
        await run_with_cancel(q, "Загружаю статистику Стролла...", fmt_stroll_stats()); return

    # ── Результаты (квали/гонка) — из текущего уикенда ───────────────────────
    if data == "res:quali":
        wks = build_weekends(); cw = cur_weekend(wks) or nxt_weekend(wks)
        await run_with_cancel(q, "Загружаю результаты квалификации...", fmt_quali(cw),
                              reply_kb_fn=lambda: cur_weekend_kb(cw)); return

    if data == "res:race":
        wks = build_weekends(); cw = cur_weekend(wks) or nxt_weekend(wks)
        await run_with_cancel(q, "Загружаю результаты гонки...", fmt_race(cw),
                              reply_kb_fn=lambda: cur_weekend_kb(cw)); return

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
    app=(Application.builder()
         .token(BOT_TOKEN)
         .connect_timeout(20.0)
         .read_timeout(30.0)
         .write_timeout(20.0)
         .pool_timeout(10.0)
         .post_init(post_init)
         .post_shutdown(post_shutdown)
         .build())
    app.add_handler(CommandHandler("start",cmd_start))
    app.add_handler(CommandHandler("status",cmd_status))
    app.add_handler(CallbackQueryHandler(on_cb))
    app.add_handler(MessageHandler(filters.Text(["🏠 Меню"]),on_menu))
    app.run_polling(drop_pending_updates=True)

if __name__=="__main__":
    main()
