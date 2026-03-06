from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
import pytz
from icalendar import Calendar
from constants import session_label, country_flag, MONTHS_RU

ICS_FILE = Path(__file__).parent / "f1_2026.ics"
MSK = pytz.timezone("Europe/Moscow")

@dataclass
class Session:
    summary: str
    location: str
    start_utc: datetime

    @property
    def start_msk(self): return self.start_utc.astimezone(MSK)
    @property
    def emoji(self): return session_label(self.summary)[0]
    @property
    def label(self): return session_label(self.summary)[1]
    @property
    def is_race(self): return "гонка" in self.summary.lower() and "спринт" not in self.summary.lower()
    @property
    def is_qualifying(self): return "квалификация" in self.summary.lower() and "спринту" not in self.summary.lower()
    @property
    def is_sprint(self): return "спринт" in self.summary.lower()

@dataclass
class RaceWeekend:
    gp_name: str
    location: str
    sessions: list = field(default_factory=list)

    @property
    def country(self):
        parts = self.location.split(",")
        return parts[-1].strip() if len(parts) > 1 else self.location
    @property
    def city(self): return self.location.split(",")[0].strip()
    @property
    def flag(self): return country_flag(self.location)
    @property
    def start_utc(self): return self.sessions[0].start_utc
    @property
    def end_utc(self): return self.sessions[-1].start_utc
    @property
    def month(self): return self.start_utc.astimezone(MSK).month
    @property
    def start_msk(self): return self.start_utc.astimezone(MSK)
    @property
    def end_msk(self): return self.end_utc.astimezone(MSK)
    def race_session(self):
        return next((s for s in self.sessions if s.is_race), None)
    def quali_session(self):
        return next((s for s in self.sessions if s.is_qualifying), None)

def _parse_raw():
    sessions = []
    with open(ICS_FILE, "rb") as f:
        cal = Calendar.from_ical(f.read())
    for comp in cal.walk():
        if comp.name != "VEVENT": continue
        summary  = str(comp.get("SUMMARY",""))
        location = str(comp.get("LOCATION","")).replace("\\,",",")
        start    = comp.get("DTSTART").dt
        if not isinstance(start, datetime):
            start = datetime(start.year, start.month, start.day, tzinfo=pytz.utc)
        elif start.tzinfo is None:
            start = pytz.utc.localize(start)
        sessions.append(Session(summary, location, start))
    sessions.sort(key=lambda s: s.start_utc)
    return sessions

def get_all_sessions(): return _parse_raw()

def get_race_weekends():
    weekends = {}
    for s in _parse_raw():
        gp = s.summary.split(".")[0].strip()
        if gp not in weekends:
            weekends[gp] = RaceWeekend(gp_name=gp, location=s.location)
        weekends[gp].sessions.append(s)
    return list(weekends.values())

def get_weekends_by_month():
    by_month = {}
    for wk in get_race_weekends():
        by_month.setdefault(wk.month, []).append(wk)
    return by_month

def get_available_months(): return sorted(get_weekends_by_month().keys())

def get_current_or_next_weekend():
    now = datetime.now(tz=pytz.utc)
    for wk in get_race_weekends():
        if wk.start_utc <= now <= wk.end_utc + timedelta(hours=3):
            return wk
    for wk in get_race_weekends():
        if wk.start_utc > now:
            return wk
    return None

def get_next_session():
    now = datetime.now(tz=pytz.utc)
    for s in get_all_sessions():
        if s.start_utc > now: return s
    return None
