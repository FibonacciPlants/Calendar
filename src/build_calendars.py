import yaml, requests, re
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime, timedelta
from dateutil import tz
from icalendar import Calendar as ICal, Event as ICalEvent
from icalendar import cal as ical_mod

ROOT = Path(__file__).resolve().parents[1]
SITE = ROOT / "site"

TZNAME = "America/Chicago"
TZ = tz.gettz(TZNAME)
WINDOW_START = datetime.now(TZ) - timedelta(days=7)  # small backfill
WINDOW_END = datetime(2026, 6, 30, 23, 59, tzinfo=TZ)  # FIFA window end

def to_local(dt):
    if dt.tzinfo is None:
        return dt.replace(tzinfo=TZ)
    return dt.astimezone(TZ)

def parse_dt(s):
    # accept 'YYYY-MM-DD' or ISO 'YYYY-MM-DDTHH:MM'
    if "T" in s:
        dt = datetime.fromisoformat(s)
        return to_local(dt)
    d = datetime.fromisoformat(s)
    return to_local(d.replace(hour=0, minute=0, second=0))

def new_cal(name):
    cal = ICal()
    cal.add("prodid", "-//DFW Influx Calendars//EN")
    cal.add("version", "2.0")
    cal.add("calscale", "GREGORIAN")
    cal.add("x-wr-calname", name)
    cal.add("x-wr-timezone", TZNAME)
    return cal

def add_event(cal, summary, start, end=None, location=None, description=None):
    # Filter to window
    if start is None:
        return
    if start < WINDOW_START or start > WINDOW_END:
        return
    ev = ICalEvent()
    ev.add("summary", summary)
    ev.add("dtstart", start)
    ev.add("dtend", (end or (start + timedelta(hours=2))))
    if location:
        ev.add("location", location)
    if description:
        ev.add("description", description)
    uid = f"{hash((summary, start.isoformat(), (end.isoformat() if end else ''), location or ''))}@dfw"
    ev.add("uid", uid)
    cal.add_component(ev)

# ---------- SOURCE HELPERS ----------

def ics_to_items(url, fallback_location=None, category_hint=None):
    items = []
    r = requests.get(url, timeout=25)
    r.raise_for_status()
    cal = ical_mod.Calendar.from_ical(r.content)
    for comp in cal.walk():
        if comp.name != "VEVENT":
            continue
        summary = str(comp.get("summary", "Event"))
        dtstart = comp.get("dtstart")
        dtend = comp.get("dtend")
        loc = str(comp.get("location", "")) or fallback_location
        desc = str(comp.get("description", ""))
        # normalize datetimes
        start = None
        end = None
        if hasattr(dtstart, "dt"):
            dtv = dtstart.dt
            if isinstance(dtv, datetime):
                start = to_local(dtv)
            else:  # date
                start = to_local(datetime(dtv.year, dtv.month, dtv.day))
        if hasattr(dtend, "dt"):
            dtv = dtend.dt
            if isinstance(dtv, datetime):
                end = to_local(dtv)
            else:  # date
                end = to_local(datetime(dtv.year, dtv.month, dtv.day))
        items.append(dict(summary=summary, start=start, end=end, location=loc, description=desc))
    return items

def text_clean(x):
    return re.sub(r"\s+", " ", (x or "").strip())

# AAC (American Airlines Center)
def scrape_aac(url):
    out = []
    r = requests.get(url, timeout=25); r.raise_for_status()
    s = BeautifulSoup(r.text, "lxml")
    for card in s.select("[data-component='EventCard'], article"):
        title_el = card.find(["h2","h3"])
        when = card.find("time")
        if not title_el or not when or not when.has_attr("datetime"): 
            continue
        title = text_clean(title_el.get_text(" ", strip=True))
        start = datetime.fromisoformat(when["datetime"][:16])
        start = to_local(start)
        out.append(dict(summary=title, start=start, location="American Airlines Center, Dallas", description=url))
    return out

# AT&T Stadium (broad; fallback to 7pm if only date text)
def scrape_att_stadium(url):
    out = []
    r = requests.get(url, timeout=25); r.raise_for_status()
    s = BeautifulSoup(r.text, "lxml")
    for ev in s.select("article, .event, .events-listing .event, .event-listing .event"):
        h = ev.find(["h2","h3"])
        t = ev.find("time")
        if not h:
            continue
        title = text_clean(h.get_text(" ", strip=True))
        if t and t.has_attr("datetime"):
            try:
                start = datetime.fromisoformat(t["datetime"][:16]); start = to_local(start)
            except Exception:
                continue
        else:
            # date text like "September 28, 2025"
            date_el = ev.find(class_=re.compile("date", re.I)) or ev
            txt = text_clean(date_el.get_text(" ", strip=True))
            m = re.search(r"([A-Za-z]+ \d{1,2}, \d{4})", txt)
            if not m: 
                continue
            try:
                d = datetime.strptime(m.group(1), "%B %d, %Y")
                start = to_local(d.replace(hour=19, minute=0))
            except Exception:
                continue
        out.append(dict(summary=title, start=start, location="AT&T Stadium, Arlington", description=url))
    return out

# Live Nation venues (Dos Equis, Echo Lounge, House of Blues Dallas, Texas Trust CU Theatre)
def scrape_livenation(url, venue_name):
    out = []
    r = requests.get(url, timeout=25); r.raise_for_status()
    s = BeautifulSoup(r.text, "lxml")
    cards = s.select("[data-testid='eventCard']") or s.select("article")
    for card in cards:
        t = card.find(["h2","h3"])
        d = card.find(attrs={"data-testid": "eventDateTime"}) or card.find("time")
        title = text_clean(t.get_text()) if t else None
        when_txt = text_clean(d.get_text()) if (d and not d.has_attr("datetime")) else (d["datetime"] if d and d.has_attr("datetime") else None)
        if not (title and when_txt): 
            continue
        # Try different formats
        start = None
        # ISO in datetime attr
        if d and d.has_attr("datetime"):
            try:
                start = datetime.fromisoformat(when_txt[:16]); start = to_local(start)
            except Exception:
                pass
        if not start:
            # e.g., "Mon Sep 1, 2025 · 7:30 PM"
            when_txt = when_txt.replace("·", " ")
            m = re.search(r"([A-Za-z]{3})\s+([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{4}).*?(\d{1,2}:\d{2}\s*[AP]M)", when_txt)
            if m:
                try:
                    start = datetime.strptime(f"{m.group(2)} {m.group(3)} {m.group(4)} {m.group(5).upper().replace(' ', '')}", "%b %d %Y %I:%M%p")
                    start = to_local(start)
                except Exception:
                    pass
        if not start: 
            continue
        out.append(dict(summary=title, start=start, location=venue_name, description=url))
    return out

# AXS / venue pages (The Factory & The Studio often list via AXS)
def scrape_axs(url, venue_name):
    out = []
    r = requests.get(url, timeout=25); r.raise_for_status()
    s = BeautifulSoup(r.text, "lxml")
    for card in s.select("article, .Card__Content, .event-card"):
        t = card.find(["h2","h3"])
        when = card.find("time")
        title = text_clean(t.get_text()) if t else None
        when_iso = when["datetime"] if (when and when.has_attr("datetime")) else None
        if not (title and when_iso): 
            continue
        try:
            start = datetime.fromisoformat(when_iso[:16]); start = to_local(start)
        except Exception:
            continue
        out.append(dict(summary=title, start=start, location=venue_name, description=url))
    return out

# Generic CSS selector source
def scrape_selector(url, container, title_sel, dt_sel, dt_attr=None, venue_name=None, default_time="19:00"):
    out = []
    r = requests.get(url, timeout=25); r.raise_for_status()
    s = BeautifulSoup(r.text, "lxml")
    for box in s.select(container):
        t = box.select_one(title_sel)
        d = box.select_one(dt_sel)
        if not t or not d:
            continue
        title = text_clean(t.get_text())
        start = None
        if dt_attr and d.has_attr(dt_attr):
            try:
                start = datetime.fromisoformat(d[dt_attr][:16]); start = to_local(start)
            except Exception:
                start = None
        if not start:
            txt = text_clean(d.get_text())
            # Try "September 28, 2025" or "Sep 28, 2025"
            m = re.search(r"([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})", txt)
            if m:
                try:
                    base = datetime.strptime(m.group(1), "%B %d, %Y")
                except Exception:
                    try:
                        base = datetime.strptime(m.group(1), "%b %d, %Y")
                    except Exception:
                        base = None
                if base:
                    hh, mm = default_time.split(":")
                    start = to_local(base.replace(hour=int(hh), minute=int(mm)))
        if not start:
            continue
        out.append(dict(summary=title, start=start, location=venue_name or "", description=url))
    return out

# -------- BUILD --------

def fetch_from_sources(cfg):
    by_cat = {k: [] for k in ["sports","music","conferences","arts","festivals","specials"]}
    for src in cfg.get("sources", []):
        stype = src.get("type")
        cat = src.get("category")
        url = src.get("url")
        try:
            if stype == "ics":
                items = ics_to_items(url, fallback_location=src.get("venue"))
            elif stype == "aac":
                items = scrape_aac(url)
            elif stype == "att_stadium":
                items = scrape_att_stadium(url)
            elif stype == "livenation":
                items = scrape_livenation(url, src.get("venue","Dallas Venue"))
            elif stype == "axs":
                items = scrape_axs(url, src.get("venue","Dallas Venue"))
            elif stype == "selector":
                items = scrape_selector(
                    url=url,
                    container=src.get("container"),
                    title_sel=src.get("title"),
                    dt_sel=src.get("datetime"),
                    dt_attr=src.get("dt_attr"),
                    venue_name=src.get("venue"),
                    default_time=src.get("default_time","19:00")
                )
            else:
                items = []
        except Exception as e:
            print(f"[warn] source failed: {url} -> {e}")
            items = []
        for it in items:
            # normalize keys
            if isinstance(it.get("start"), str):
                it["start"] = parse_dt(it["start"])
            by_cat.get(cat, []).append(it)
    return by_cat

def dedupe(items):
    seen = set()
    out = []
    for it in items:
        key = (it["summary"], it["start"].isoformat(), it.get("location",""))
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out

def main():
    cfg = yaml.safe_load((ROOT / "data" / "events.yaml").read_text(encoding="utf-8"))
    cats = ["sports","music","conferences","arts","festivals","specials"]
    cals = {c: new_cal(f"DFW — {c.title()}") for c in cats}
    master = new_cal("DFW — Influx Events (Master)")

    # 1) fetch from sources
    scraped = fetch_from_sources(cfg)

    # 2) add curated items
    for cat in cats:
        scraped.setdefault(cat, [])
        for it in (cfg.get("events", {}).get(cat, []) or []):
            if isinstance(it.get("start"), str):
                it["start"] = parse_dt(it["start"])
            scraped[cat].append(it)

    # 3) dedupe + add to calendars
    for cat in cats:
        bucket = dedupe(scraped[cat])
        for it in bucket:
            add_event(cals[cat], it["summary"], it["start"], it.get("end"), it.get("location"), it.get("description"))
            add_event(master, it["summary"], it["start"], it.get("end"), it.get("location"), it.get("description"))

    # 4) write feeds
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
