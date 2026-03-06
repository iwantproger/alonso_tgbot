import logging
import aiohttp
import pytz

log = logging.getLogger(__name__)
OPENF1  = "https://api.openf1.org/v1"
JOLPICA = "https://api.jolpi.ca/ergast/f1"
WTTR    = "https://wttr.in"
_TIMEOUT = aiohttp.ClientTimeout(total=10)

async def _get(url, params=None):
    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as s:
            async with s.get(url, params=params) as r:
                if r.status == 200:
                    return await r.json(content_type=None)
    except Exception as e:
        log.debug("HTTP %s: %s", url, e)
    return None

async def of1(endpoint, **kw):
    data = await _get(f"{OPENF1}/{endpoint}", params=kw or None)
    return data if isinstance(data, list) else None

async def get_latest_session():
    rows = await of1("sessions", session_key="latest")
    return rows[-1] if rows else None

async def get_weather(session_key="latest"):
    rows = await of1("weather", session_key=session_key)
    return rows[-1] if rows else None

async def get_race_control(session_key="latest"):
    rows = await of1("race_control", session_key=session_key)
    return rows or []

async def get_pit_stops(session_key="latest"):
    rows = await of1("pit", session_key=session_key)
    return rows or []

async def get_stints(session_key="latest"):
    rows = await of1("stints", session_key=session_key)
    return rows or []

async def get_positions(session_key="latest"):
    rows = await of1("position", session_key=session_key)
    return rows or []

async def get_drivers(session_key="latest"):
    rows = await of1("drivers", session_key=session_key) or []
    return {r["driver_number"]: r for r in rows if "driver_number" in r}

async def get_laps(session_key="latest", driver_number=None):
    kw = {"session_key": session_key}
    if driver_number: kw["driver_number"] = driver_number
    rows = await of1("laps", **kw)
    return rows or []

async def jol(path):
    data = await _get(f"{JOLPICA}/{path}")
    if isinstance(data, dict): return data.get("MRData")
    return None

async def get_driver_standings(season="current"):
    data = await jol(f"{season}/driverStandings.json?limit=30")
    try: return data["StandingsTable"]["StandingsLists"][0]["DriverStandings"]
    except: return []

async def get_constructor_standings(season="current"):
    data = await jol(f"{season}/constructorStandings.json?limit=15")
    try: return data["StandingsTable"]["StandingsLists"][0]["ConstructorStandings"]
    except: return []

async def get_last_qualifying():
    data = await jol("current/last/qualifying.json?limit=25")
    try: return data["RaceTable"]["Races"][0]["QualifyingResults"]
    except: return []

async def get_last_race_results():
    data = await jol("current/last/results.json?limit=25")
    try:
        race = data["RaceTable"]["Races"][0]
        return race, race["Results"]
    except: return None, []

async def get_forecast(city):
    clean = city.replace(",","").replace(" ","+")
    data  = await _get(f"{WTTR}/{clean}?format=j1")
    if not data: return None
    try:
        cur = data["current_condition"][0]
        return {"temp_c":int(cur["temp_C"]),"feels_like":int(cur["FeelsLikeC"]),
                "description":cur["weatherDesc"][0]["value"],
                "humidity":int(cur["humidity"]),"wind_kmh":int(cur["windspeedKmph"]),
                "precip_mm":float(cur["precipMM"])}
    except: return None

def fmt_weather(w):
    if not w: return None
    rain = "🌧 Дождь" if w["precip_mm"] > 0 else "☀️ Сухо"
    return f"{rain} | 🌡 {w['temp_c']}°C (ощущается {w['feels_like']}°C) | 💧 {w['humidity']}% | 💨 {w['wind_kmh']} км/ч"

def fmt_openf1_weather(w):
    if not w: return None
    rain = "🌧 Дождь" if w.get("rainfall") else "☀️ Сухо"
    return f"{rain} | 🌡 Воздух {w.get('air_temperature','?')}°C | 🛣 Трасса {w.get('track_temperature','?')}°C | 💨 {w.get('wind_speed','?')} м/с"
