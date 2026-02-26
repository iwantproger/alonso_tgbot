#!/usr/bin/env python3
"""
🏎️ F1 2026 Telegram Notification Bot
Уведомления о гонках Формулы 1 за 30 минут и в момент старта.
Время: московское (МСК, UTC+3).
"""

import json
import logging
import math
import os
from datetime import datetime, timedelta
from pathlib import Path

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from icalendar import Calendar
from telegram import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

# ──────────────────────────────────────────────────────────────────────────────
#  Конфигурация
# ──────────────────────────────────────────────────────────────────────────────
BOT_TOKEN   = os.environ["BOT_TOKEN"]            # Обязательно через env
STORAGE     = Path(__file__).parent / "data" / "subscribers.json"
ICS_FILE    = Path(__file__).parent / "f1_2026.ics"
MSK         = pytz.timezone("Europe/Moscow")
EVENTS_PAGE = 8   # событий на одной странице

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

STORAGE.parent.mkdir(exist_ok=True)

# ──────────────────────────────────────────────────────────────────────────────
#  Хранилище подписчиков (JSON-файл)
# ──────────────────────────────────────────────────────────────────────────────
def _load() -> dict:
    if STORAGE.exists():
        return json.loads(STORAGE.read_text(encoding="utf-8"))
    return {"users": [], "chats": []}

def _save(d: dict):
    STORAGE.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")

def subscribe(chat_id: int, is_private: bool) -> bool:
    d = _load(); key = "users" if is_private else "chats"
    if chat_id not in d[key]:
        d[key].append(chat_id); _save(d); return True
    return False

def unsubscribe(chat_id: int, is_private: bool) -> bool:
    d = _load(); key = "users" if is_private else "chats"
    if chat_id in d[key]:
        d[key].remove(chat_id); _save(d); return True
    return False

def all_subscribers() -> list[int]:
    d = _load(); return d["users"] + d["chats"]

# ──────────────────────────────────────────────────────────────────────────────
#  Парсинг ICS-календаря
# ──────────────────────────────────────────────────────────────────────────────
def parse_events() -> list[dict]:
    events = []
    with open(ICS_FILE, "rb") as f:
        cal = Calendar.from_ical(f.read())

    for comp in cal.walk():
        if comp.name != "VEVENT":
            continue
        summary  = str(comp.get("SUMMARY", ""))
        location = str(comp.get("LOCATION", "")).replace("\\,", ",")
        start    = comp.get("DTSTART").dt

        if not isinstance(start, datetime):
            start = datetime(start.year, start.month, start.day, tzinfo=pytz.utc)
        elif start.tzinfo is None:
            start = pytz.utc.localize(start)

        events.append({"summary": summary, "location": location, "start_utc": start})

    events.sort(key=lambda e: e["start_utc"])
    return events

# ──────────────────────────────────────────────────────────────────────────────
#  Форматирование
# ──────────────────────────────────────────────────────────────────────────────
MONTHS_RU = {
    1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",
    7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"
}

def _session_emoji(summary: str) -> str:
    s = summary.lower()
    if "гонка" in s:           return "🏁"
    if "квалификация к" in s:  return "⚡"
    if "спринт" in s:          return "⚡"
    if "квалификация" in s:    return "🔥"
    if "свободных" in s:       return "🔧"
    return "🏎️"

def _msk_str(utc_dt: datetime) -> str:
    msk = utc_dt.astimezone(MSK)
    return f"{msk.day} {MONTHS_RU[msk.month]}, {msk.strftime('%H:%M')} МСК"

def _notify_text(event: dict, remind: bool) -> str:
    emoji = _session_emoji(event["summary"])
    name  = event["summary"]
    loc   = event["location"]
    t     = _msk_str(event["start_utc"])
    if remind:
        return (
            f"{emoji} <b>Через 30 минут!</b>\n\n"
            f"<b>{name}</b>\n"
            f"📍 {loc}\n"
            f"🕐 {t}"
        )
    return (
        f"{emoji} <b>СТАРТ! Прямо сейчас!</b>\n\n"
        f"<b>{name}</b>\n"
        f"📍 {loc}\n"
        f"🕐 {t}"
    )

def _event_line(e: dict, idx: int | None = None) -> str:
    emoji  = _session_emoji(e["summary"])
    msk    = e["start_utc"].astimezone(MSK)
    date   = f"{msk.day:02d}.{msk.month:02d}"
    time   = msk.strftime("%H:%M")
    # Убираем "Гран При XXX. " чтобы не дублировать
    summary = e["summary"]
    # Укорачиваем название сессии
    short_map = {
        "1-я сессия свободных заездов": "СП-1",
        "2-я сессия свободных заездов": "СП-2",
        "3-я сессия свободных заездов": "СП-3",
        "Квалификация к спринту":       "Кв.Спринта",
        "Спринт":                        "Спринт",
        "Квалификация":                  "Квалификация",
        "Гонка":                         "Гонка",
    }
    session = summary
    for long, short in short_map.items():
        if long in summary:
            session = short; break

    # Страна
    loc_parts = e["location"].split(",")
    city    = loc_parts[0].strip()
    country = loc_parts[-1].strip() if len(loc_parts) > 1 else city

    prefix = f"{idx}. " if idx is not None else ""
    return f"{prefix}{emoji} <b>{date} {time}</b> — {country}, {city} — {session}"

# ──────────────────────────────────────────────────────────────────────────────
#  Планировщик уведомлений
# ──────────────────────────────────────────────────────────────────────────────
scheduler = AsyncIOScheduler(timezone=pytz.utc)
_app: Application | None = None

async def _send_all(event: dict, remind: bool):
    if _app is None:
        return
    text = _notify_text(event, remind)
    for cid in all_subscribers():
        try:
            await _app.bot.send_message(cid, text, parse_mode="HTML")
        except Exception as exc:
            log.warning("Ошибка отправки в %s: %s", cid, exc)

def _schedule_events():
    events = parse_events()
    now    = datetime.now(tz=pytz.utc)
    count  = 0
    for ev in events:
        start = ev["start_utc"]
        # За 30 минут
        t30 = start - timedelta(minutes=30)
        if t30 > now:
            scheduler.add_job(
                _send_all, "date", run_date=t30, args=[ev, True],
                id=f"r_{ev['summary']}_{start.isoformat()}", replace_existing=True
            )
            count += 1
        # В момент старта
        if start > now:
            scheduler.add_job(
                _send_all, "date", run_date=start, args=[ev, False],
                id=f"s_{ev['summary']}_{start.isoformat()}", replace_existing=True
            )
            count += 1
    log.info("Запланировано %d уведомлений (%d событий)", count, len(events))

# ──────────────────────────────────────────────────────────────────────────────
#  Хэлперы для пагинации
# ──────────────────────────────────────────────────────────────────────────────
def _events_page_text(events: list[dict], page: int, total_pages: int) -> str:
    start = page * EVENTS_PAGE
    chunk = events[start : start + EVENTS_PAGE]
    lines = [f"📋 <b>Календарь F1 2026 — стр. {page+1}/{total_pages}</b>\n"]
    for i, ev in enumerate(chunk, start=start+1):
        lines.append(_event_line(ev, i))
    lines.append(f"\n<i>Всего событий: {len(events)} • Время МСК (UTC+3)</i>")
    return "\n".join(lines)

def _events_keyboard(page: int, total_pages: int, mode: str = "all") -> InlineKeyboardMarkup:
    row = []
    if page > 0:
        row.append(InlineKeyboardButton("◀️ Назад", callback_data=f"ev:{mode}:{page-1}"))
    if page < total_pages - 1:
        row.append(InlineKeyboardButton("Вперёд ▶️", callback_data=f"ev:{mode}:{page+1}"))
    return InlineKeyboardMarkup([row]) if row else None

def _filter_events(mode: str) -> list[dict]:
    all_ev = parse_events()
    now    = datetime.now(tz=pytz.utc)
    if mode == "upcoming":
        return [e for e in all_ev if e["start_utc"] > now]
    if mode == "races":
        return [e for e in all_ev if "гонка" in e["summary"].lower()]
    if mode == "quali":
        return [
            e for e in all_ev
            if "квалификация" in e["summary"].lower()
            and "спринту" not in e["summary"].lower()
        ]
    return all_ev  # "all"

# ──────────────────────────────────────────────────────────────────────────────
#  Команды
# ──────────────────────────────────────────────────────────────────────────────
async def cmd_start(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat = upd.effective_chat
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подписаться на уведомления", callback_data="sub")],
        [
            InlineKeyboardButton("📅 Ближайшие события",  callback_data="ev:upcoming:0"),
            InlineKeyboardButton("🗓 Весь календарь",      callback_data="ev:all:0"),
        ],
        [
            InlineKeyboardButton("🏁 Только гонки",       callback_data="ev:races:0"),
            InlineKeyboardButton("🔥 Только квалификации", callback_data="ev:quali:0"),
        ],
        [InlineKeyboardButton("❌ Отписаться",             callback_data="unsub")],
    ])
    is_group = chat.type != "private"
    text = (
        "🏎️ <b>F1 2026 — бот уведомлений</b>\n\n"
        + ("Добавь этот чат в подписку, чтобы получать уведомления о Ф1!\n\n" if is_group else
           "Я пришлю уведомление <b>за 30 минут</b> и <b>ровно в момент</b> каждой сессии!\n\n")
        + "⏰ Время — московское (МСК, UTC+3)\n"
        + "📍 120 событий сезона 2026\n\n"
        + "Выбери действие:"
    )
    await upd.message.reply_text(text, parse_mode="HTML", reply_markup=kb)

async def cmd_subscribe(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat = upd.effective_chat
    ok   = subscribe(chat.id, chat.type == "private")
    txt  = ("✅ <b>Подписка оформлена!</b>\nБуду присылать уведомления за 30 мин и в момент старта 🏎️"
            if ok else "ℹ️ Ты уже подписан(а)!")
    await upd.message.reply_text(txt, parse_mode="HTML")

async def cmd_unsubscribe(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat = upd.effective_chat
    ok   = unsubscribe(chat.id, chat.type == "private")
    txt  = "❌ Подписка отменена." if ok else "ℹ️ Ты не был(а) подписан(а)."
    await upd.message.reply_text(txt)

async def cmd_schedule(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Ближайшие 8 событий."""
    events = [e for e in parse_events() if e["start_utc"] > datetime.now(tz=pytz.utc)]
    if not events:
        await upd.message.reply_text("😔 Предстоящих событий нет.")
        return
    total = math.ceil(len(events) / EVENTS_PAGE)
    await upd.message.reply_text(
        _events_page_text(events[:EVENTS_PAGE], 0, total),
        parse_mode="HTML",
        reply_markup=_events_keyboard(0, total, "upcoming")
    )

async def cmd_events(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Полный хронологический список."""
    events = parse_events()
    total  = math.ceil(len(events) / EVENTS_PAGE)
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔜 Предстоящие",          callback_data="ev:upcoming:0"),
            InlineKeyboardButton("🏁 Гонки",                callback_data="ev:races:0"),
        ],
        [
            InlineKeyboardButton("🔥 Квалификации",         callback_data="ev:quali:0"),
            InlineKeyboardButton("📋 Все 120 событий",      callback_data="ev:all:0"),
        ],
    ])
    await upd.message.reply_text(
        "🗓 <b>Выбери, какой список показать:</b>",
        parse_mode="HTML", reply_markup=kb
    )

async def cmd_status(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    d     = _load()
    jobs  = len(scheduler.get_jobs())
    now   = datetime.now(tz=MSK)
    await upd.message.reply_text(
        f"📊 <b>Статус бота</b>\n\n"
        f"👤 Личных подписок: {len(d['users'])}\n"
        f"💬 Чатов/групп: {len(d['chats'])}\n"
        f"⏰ Запланированных уведомлений: {jobs}\n"
        f"🕐 Время сервера: {now.strftime('%d.%m.%Y %H:%M')} МСК",
        parse_mode="HTML"
    )

# ──────────────────────────────────────────────────────────────────────────────
#  Callback-кнопки
# ──────────────────────────────────────────────────────────────────────────────
async def on_callback(upd: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q: CallbackQuery = upd.callback_query
    await q.answer()
    data = q.data
    chat = upd.effective_chat

    if data == "sub":
        ok  = subscribe(chat.id, chat.type == "private")
        txt = ("✅ <b>Подписка оформлена!</b>\nПришлю за 30 мин и в момент старта 🏎️"
               if ok else "ℹ️ Ты уже подписан(а)!")
        await q.edit_message_text(txt + "\n\n/events — посмотреть календарь", parse_mode="HTML")
        return

    if data == "unsub":
        unsubscribe(chat.id, chat.type == "private")
        await q.edit_message_text("❌ Подписка отменена.")
        return

    if data.startswith("ev:"):
        _, mode, page_str = data.split(":")
        page   = int(page_str)
        events = _filter_events(mode)
        if not events:
            await q.edit_message_text("😔 Событий нет.")
            return
        total  = math.ceil(len(events) / EVENTS_PAGE)
        page   = max(0, min(page, total - 1))
        text   = _events_page_text(events, page, total)
        kb     = _events_keyboard(page, total, mode)
        await q.edit_message_text(text, parse_mode="HTML", reply_markup=kb)
        return

# ──────────────────────────────────────────────────────────────────────────────
#  Запуск
# ──────────────────────────────────────────────────────────────────────────────
def main():
    global _app
    app   = Application.builder().token(BOT_TOKEN).build()
    _app  = app

    app.add_handler(CommandHandler("start",       cmd_start))
    app.add_handler(CommandHandler("subscribe",   cmd_subscribe))
    app.add_handler(CommandHandler("unsubscribe", cmd_unsubscribe))
    app.add_handler(CommandHandler("schedule",    cmd_schedule))
    app.add_handler(CommandHandler("events",      cmd_events))
    app.add_handler(CommandHandler("status",      cmd_status))
    app.add_handler(CallbackQueryHandler(on_callback))

    _schedule_events()
    scheduler.start()

    log.info("🏎️  F1 2026 Bot запущен!")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
