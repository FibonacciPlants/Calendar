import yaml
from pathlib import Path
from datetime import datetime, timedelta
from dateutil import tz
from icalendar import Calendar, Event

ROOT = Path(__file__).resolve().parents[1]
SITE = ROOT / "site"

def parse_dt(s, tzname):
    tzinfo = tz.gettz(tzname)
    if "T" in s:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tzinfo)
        return dt
    else:
        dt = datetime.fromisoformat(s)
        return dt.replace(tzinfo=tzinfo, hour=0, minute=0, second=0)

def new_cal(name):
    cal = Calendar()
    cal.add("prodid", "-//DFW Influx Calendars//EN")
    cal.add("version", "2.0")
    cal.add("calscale", "GREGORIAN")
    cal.add("x-wr-calname", name)
    cal.add("x-wr-timezone", "America/Chicago")
    return cal

def add_event(cal, item, tzname):
    ev = Event()
    summary = item.get("summary", "Untitled")
    start = item.get("start")
    end = item.get("end")
    allday = item.get("all_day", False)

    if not start:
        return
    if allday or len(start) == 10:
        dtstart = datetime.fromisoformat(start).date()
        ev.add("dtstart", dtstart)
        dtend = datetime.fromisoformat(end).date() if end else dtstart + timedelta(days=1)
        ev.add("dtend", dtend)
    else:
        dtstart = parse_dt(start, tzname)
        dtend = parse_dt(end, tzname) if end else dtstart + timedelta(hours=2)
        ev.add("dtstart", dtstart)
        ev.add("dtend", dtend)

    ev.add("summary", summary)
    if item.get("location"):
        ev.add("location", item["location"])
    if item.get("description"):
        ev.add("description", item["description"])
    uid = f"{hash((summary, start, end, item.get('location','')))}@dfw-influx"
    ev.add("uid", uid)
    cal.add_component(ev)

def main():
    cfg = yaml.safe_load((ROOT / "data" / "events.yaml").read_text(encoding="utf-8"))
    tzname = cfg.get("timezone", "America/Chicago")
    cats = ["sports","music","conferences","arts","festivals","specials"]
    cals = {c: new_cal(f"DFW — {c.title()}") for c in cats}
    master = new_cal("DFW — Influx Events (Master)")

    for cat in cats:
        for it in cfg.get("events", {}).get(cat, []) or []:
            add_event(cals[cat], it, tzname)
            add_event(master, it, tzname)

    SITE.mkdir(parents=True, exist_ok=True)
    (SITE / "sports.ics").write_bytes(cals["sports"].to_ical())
    (SITE / "music.ics").write_bytes(cals["music"].to_ical())
    (SITE / "conferences.ics").write_bytes(cals["conferences"].to_ical())
    (SITE / "arts.ics").write_bytes(cals["arts"].to_ical())
    (SITE / "festivals.ics").write_bytes(cals["festivals"].to_ical())
    (SITE / "specials_worldcup.ics").write_bytes(cals["specials"].to_ical())
    (SITE / "master.ics").write_bytes(master.to_ical())

if __name__ == "__main__":
    main()
