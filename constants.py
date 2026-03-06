COUNTRY_FLAGS = {
    "австралия": "🇦🇺","китай": "🇨🇳","япония": "🇯🇵","бахрейн": "🇧🇭",
    "саудовская аравия": "🇸🇦","саудов": "🇸🇦","италия": "🇮🇹","монако": "🇲🇨",
    "канада": "🇨🇦","испания": "🇪🇸","барселона": "🇪🇸","каталония": "🇪🇸",
    "австрия": "🇦🇹","великобритания": "🇬🇧","венгрия": "🇭🇺","бельгия": "🇧🇪",
    "нидерланды": "🇳🇱","сингапур": "🇸🇬","мексика": "🇲🇽","бразилия": "🇧🇷",
    "абу-даби": "🇦🇪","азербайджан": "🇦🇿","катар": "🇶🇦","лас-вегас": "🇺🇸",
    "майами": "🇺🇸","сша": "🇺🇸","джидда": "🇸🇦",
}
def country_flag(location):
    loc = location.lower()
    for key, flag in COUNTRY_FLAGS.items():
        if key in loc:
            return flag
    return "🏁"

TIRE_EMOJI = {"SOFT":"🔴 Soft","MEDIUM":"🟡 Medium","HARD":"⚪ Hard","INTERMEDIATE":"🟢 Inter","WET":"🔵 Wet"}
def tire(compound):
    return TIRE_EMOJI.get((compound or "").upper(), f"⬜ {compound}")

FLAG_EMOJI = {"YELLOW":"🟡","RED":"🔴","GREEN":"🟢","CHEQUERED":"🏁","BLUE":"🔵","BLACK AND WHITE":"🏳️","CLEAR":"✅"}

def session_label(summary):
    s = summary.lower()
    if "гонка" in s and "спринт" not in s: return "🏁","Гонка"
    if "квалификация к спринту" in s: return "⚡","Кв. спринта"
    if "спринт" in s: return "⚡","Спринт"
    if "квалификация" in s: return "🔥","Квалификация"
    if "1-я сессия" in s: return "🔧","СП-1"
    if "2-я сессия" in s: return "🔧","СП-2"
    if "3-я сессия" in s: return "🔧","СП-3"
    return "🏎️", summary

MONTHS_RU = {1:"Январь",2:"Февраль",3:"Март",4:"Апрель",5:"Май",6:"Июнь",7:"Июль",8:"Август",9:"Сентябрь",10:"Октябрь",11:"Ноябрь",12:"Декабрь"}
MONTHS_RU_GEN = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}

TRACKED_DRIVER_NUMBER = 16
TRACKED_DRIVER_FULL   = "Шарль Леклер"
RACE_POINTS = {1:25,2:18,3:15,4:12,5:10,6:8,7:6,8:4,9:2,10:1}
