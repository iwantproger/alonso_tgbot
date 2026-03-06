from __future__ import annotations
from datetime import datetime
import pytz
from constants import MONTHS_RU, MONTHS_RU_GEN, country_flag, tire, TRACKED_DRIVER_FULL

MSK = pytz.timezone("Europe/Moscow")

def msk_full(utc_dt):
    d = utc_dt.astimezone(MSK)
    return f"{d.day} {MONTHS_RU_GEN[d.month]}, {d.strftime('%H:%M')} МСК"

def pos_medal(pos):
    return {1:"🥇",2:"🥈",3:"🥉"}.get(pos, f"<b>{pos}.</b>")

def notif_button_label(subscribed):
    return "🟢 Уведомления ВКЛ" if subscribed else "⚫ Уведомления ВЫКЛ"

_CITY_TZ = {
    "мельбурн":"Australia/Melbourne","шанхай":"Asia/Shanghai","сузука":"Asia/Tokyo",
    "сахир":"Asia/Bahrain","джидда":"Asia/Riyadh","монца":"Europe/Rome",
    "монте-карло":"Europe/Monaco","монреаль":"America/Toronto","барселона":"Europe/Madrid",
    "шпильберг":"Europe/Vienna","сильверстоун":"Europe/London","будапешт":"Europe/Budapest",
    "спа":"Europe/Brussels","зандворт":"Europe/Amsterdam","сингапур":"Asia/Singapore",
    "мехико":"America/Mexico_City","сан-паулу":"America/Sao_Paulo",
    "лас-вегас":"America/Los_Angeles","луссаил":"Asia/Qatar","яс":"Asia/Dubai",
    "баку":"Asia/Baku","майами":"America/New_York","остин":"America/Chicago",
}
def _city_tz(location):
    loc = location.lower()
    for city, tz in _CITY_TZ.items():
        if city in loc: return tz
    return None

def fmt_month_page(month, weekends):
    lines = [f"📅 <b>F1 2026 — {MONTHS_RU[month]}</b>\n"]
    for wk in weekends:
        start = wk.start_msk; end = wk.end_msk
        if start.month == end.month:
            dates = f"{start.day}–{end.day} {MONTHS_RU_GEN[start.month]}"
        else:
            dates = f"{start.day} {MONTHS_RU_GEN[start.month]}–{end.day} {MONTHS_RU_GEN[end.month]}"
        lines.append(f"\n{wk.flag} <b>{wk.gp_name}</b>")
        lines.append(f"📍 {wk.city}, {wk.country}  📅 {dates}")
        for s in wk.sessions:
            d = s.start_msk
            lines.append(f"  {s.emoji} {s.label}: <b>{d.strftime('%H:%M')} МСК</b>  <i>({d.strftime('%d.%m')})</i>")
    lines.append("\n<i>⏰ Всё время московское (UTC+3)</i>")
    return "\n".join(lines)

def fmt_weekend_detail(wk, standings=None):
    now = datetime.now(tz=pytz.utc)
    start = wk.start_msk; end = wk.end_msk
    if start.month == end.month:
        dates = f"{start.day}–{end.day} {MONTHS_RU_GEN[start.month]} 2026"
    else:
        dates = f"{start.day} {MONTHS_RU_GEN[start.month]}–{end.day} {MONTHS_RU_GEN[end.month]} 2026"
    lines = [f"{wk.flag} <b>{wk.gp_name}</b>",f"📍 {wk.city}, {wk.country}",f"📅 {dates}\n","<b>Расписание сессий:</b>"]
    for s in wk.sessions:
        d = s.start_msk
        marker = "✅" if s.start_utc < now else "▶️"
        lines.append(f"  {marker} {s.emoji} <b>{s.label}</b> — {d.strftime('%d.%m %H:%M')} МСК")
    if standings:
        lines.append("\n<b>🏆 Чемпионат пилотов (топ-10):</b>")
        for st in standings[:10]:
            drv = st["Driver"]; name = f"{drv['givenName']} {drv['familyName']}"
            pts = st["points"]; pos = int(st["position"])
            lines.append(f"  {pos_medal(pos)} {name} — <b>{pts} очк.</b>")
    return "\n".join(lines)

def fmt_pre_session(session, minutes, weather_text=None):
    flag = country_flag(session.location)
    gp   = session.summary.split(".")[0].strip()
    t    = msk_full(session.start_utc)
    city_tz = _city_tz(session.location)
    t_local = None
    if city_tz:
        try:
            ltz = pytz.timezone(city_tz)
            d   = session.start_utc.astimezone(ltz)
            t_local = d.strftime(f"%H:%M ({d.strftime('%Z')})")
        except: pass
    label = "Через 30 минут!" if minutes == 30 else "Через 5 минут!"
    lines = [f"{session.emoji} <b>{label}</b>","",f"{flag} <b>{gp}</b>",f"🏷 {session.label}",f"📍 {session.location}","",f"🕐 МСК: <b>{t}</b>"]
    if t_local: lines.append(f"🕐 Местное: {t_local}")
    if weather_text: lines += ["",f"🌤 <b>Погода:</b> {weather_text}"]
    return "\n".join(lines)

def fmt_qualifying_result(results):
    if not results: return "😔 Результаты квалификации ещё не доступны."
    lines = ["🔥 <b>Результаты квалификации</b>\n"]
    q1_out = []; q2_out = []; q3 = []
    for r in results:
        pos = int(r["position"]); drv = r["Driver"]
        name = f"{drv['givenName']} {drv['familyName']}"; con = r["Constructor"]["name"]
        best = r.get("Q3") or r.get("Q2") or r.get("Q1") or "—"
        medal = pos_medal(pos)
        lines.append(f"{medal} {name} <i>({con})</i>  <code>{best}</code>")
        if pos > 15: q1_out.append(name)
        elif pos > 10: q2_out.append(name)
    lines += ["","<b>Вылетели в Q1:</b> " + ", ".join(q1_out) if q1_out else "","<b>Вылетели в Q2:</b> " + ", ".join(q2_out) if q2_out else ""]
    lines += ["","<b>🏁 Стартовая решётка (топ-10):</b>"]
    for r in results[:10]:
        pos = int(r["position"]); drv = r["Driver"]
        lines.append(f"  <b>P{pos}</b> {drv['familyName']} ({r['Constructor']['name']})")
    return "\n".join(l for l in lines if l is not None)

def fmt_race_result(race, results, driver_standings=None):
    if not results: return "😔 Результаты гонки ещё не доступны."
    name = race.get("raceName","Гонка") if race else "Гонка"
    circuit = race.get("Circuit",{}).get("circuitName","") if race else ""
    lines = [f"🏁 <b>Результаты: {name}</b>",f"📍 {circuit}\n"]
    for r in results[:10]:
        pos = int(r["position"]); drv = r["Driver"]
        full = f"{drv['givenName']} {drv['familyName']}"; con = r["Constructor"]["name"]
        time_s = r.get("Time",{}).get("time") or r.get("status","")
        pts = r.get("points","0")
        lines.append(f"{pos_medal(pos)} {full} <i>({con})</i>  {time_s}  <b>+{pts} очк.</b>")
    fl = next((r for r in results if r.get("FastestLap",{}).get("rank")=="1"), None)
    if fl:
        lines.append(f"\n⚡ <b>Быстрый круг:</b> {fl['Driver']['familyName']} — {fl['FastestLap']['Time']['time']}")
    if driver_standings:
        lines.append("\n<b>🏆 Чемпионат после гонки (топ-5):</b>")
        for st in driver_standings[:5]:
            drv = st["Driver"]; name2 = f"{drv['givenName']} {drv['familyName']}"
            lines.append(f"  {pos_medal(int(st['position']))} {name2} — <b>{st['points']} очк.</b>")
    return "\n".join(lines)

def fmt_starting_grid(quali_results, weather_text, gp_name, t_msk, t_local=None):
    lines = [f"🏁 <b>Стартовая решётка — {gp_name}</b>",f"🕐 <b>{t_msk}</b>"]
    if t_local: lines.append(f"🕐 Местное: {t_local}")
    if weather_text: lines += ["",f"🌤 <b>Погода:</b> {weather_text}"]
    lines.append("")
    if quali_results:
        for r in quali_results:
            pos = int(r["position"]); drv = r["Driver"]
            lines.append(f"  <b>P{pos}</b> {drv['familyName']} ({r['Constructor']['name']})")
    else:
        lines.append("<i>Решётка ещё не определена</i>")
    return "\n".join(lines)

def fmt_pit_stop(pit, prev_compound, new_compound, driver_name, team):
    dur = pit.get("pit_duration"); lap = pit.get("lap_number","?")
    dur_s = f"{dur:.1f}с" if isinstance(dur, (int,float)) else "?"
    from_t = tire(prev_compound) if prev_compound else "?"
    to_t   = tire(new_compound)  if new_compound  else "?"
    return f"🔩 <b>Пит-стоп!</b>\n👤 {driver_name} ({team})\n📍 Круг {lap} | ⏱ {dur_s}\n🔄 {from_t} → {to_t}"

def fmt_race_control(msg):
    cat = msg.get("category",""); flag = msg.get("flag",""); text = msg.get("message",""); lap = msg.get("lap_number")
    lap_s = f" (круг {lap})" if lap else ""
    if cat == "SafetyCar" and "DEPLOYED" in text.upper():
        return f"🚗 <b>SAFETY CAR</b>{lap_s}\nАвтомобиль безопасности на трассе!"
    if cat == "SafetyCar" and "VIRTUAL" in text.upper():
        return f"🟡 <b>ВИРТУАЛЬНЫЙ SAFETY CAR</b>{lap_s}"
    if cat == "SafetyCar" and "WITHDRAWN" in text.upper():
        return f"✅ <b>Safety Car убран</b>{lap_s} — гонка возобновляется!"
    if flag == "RED": return f"🔴 <b>КРАСНЫЙ ФЛАГ</b>{lap_s}!\n{text}"
    if flag == "YELLOW" and cat == "Flag":
        scope = msg.get("scope","")
        return f"🟡 <b>Жёлтый флаг</b>{lap_s} ({scope})"
    if "PENALTY" in text.upper() or "TIME PENALTY" in text.upper():
        dn = msg.get("driver_number","")
        return f"⚠️ <b>Штраф!</b>\n{text}" + (f" (№{dn})" if dn else "")
    if flag == "CHEQUERED": return f"🏁 <b>Клетчатый флаг!</b> Гонка завершена!"
    return None

def fmt_fastest_lap(lap_time, driver_name, team, position=None):
    pos_s = f" (P{position})" if position else ""
    return f"⚡ <b>Новый быстрый круг!</b>\n👤 {driver_name} ({team}){pos_s}\n⏱ {lap_time}"

def fmt_finish(pos, driver_name, team, time_str):
    medals = {1:"🥇 ПОБЕДИТЕЛЬ",2:"🥈 2-е место",3:"🥉 3-е место"}
    return f"{medals.get(pos,f'P{pos}')}\n👤 {driver_name} ({team})\n⏱ {time_str}"

def fmt_leclerc_stats(quali_results, race_results, driver_name=TRACKED_DRIVER_FULL):
    lines = [f"🏎️ <b>Статистика: {driver_name}</b>\n"]
    last_name = driver_name.split()[-1].lower()
    if quali_results:
        lec = next((r for r in quali_results if last_name in r["Driver"]["familyName"].lower()), None)
        if lec:
            pos=lec["position"]; q1=lec.get("Q1","—"); q2=lec.get("Q2","—"); q3=lec.get("Q3","—")
            lines += ["🔥 <b>Квалификация:</b>",f"  Позиция: P{pos}",f"  Q1: {q1} | Q2: {q2} | Q3: {q3}"]
    if race_results:
        lec = next((r for r in race_results if last_name in r["Driver"]["familyName"].lower()), None)
        if lec:
            pos=lec["position"]; grid=lec.get("grid","?"); status=lec.get("status","Финишировал")
            pts=lec.get("points","0"); time_s=lec.get("Time",{}).get("time",""); laps=lec.get("laps","?")
            fl=lec.get("FastestLap",{})
            lines += ["","🏁 <b>Гонка:</b>",f"  Старт: P{grid} → Финиш: P{pos}",f"  Статус: {status}",f"  Кругов: {laps} | Очков: +{pts}"]
            if time_s: lines.append(f"  Время: {time_s}")
            if fl.get("Time"): lines.append(f"  Быстрый круг: {fl['Time']['time']}")
    if len(lines) == 1: lines.append("Данных пока нет.")
    return "\n".join(lines)
