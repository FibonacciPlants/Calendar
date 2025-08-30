import yaml, requests
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime, timedelta
from dateutil import tz
from icalendar import Calendar, Event

ROOT = Path(__file__).resolve().parents[1]
SITE = ROOT / "site"

def parse_dt(s, tzname):
    t = tz.gettz(tzname)
    if "T" in s:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=t)
    # date-only
    d = datetime.fromisoformat(s).date()
    return datetime(d.year, d.month, d.day, 0, 0, tzinfo=t)

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
        dtend = (datetime.fromisoformat(end).date() if end else dtstart + timedelta(days=1))
        ev.add("dtstart", dtstart); ev.add("dtend", dtend)
    else:
        dtstart = parse_dt(start, tzname)
        dtend = parse_dt(end, tzname) if end else dtstart + timedelta(hours=2)
        ev.add("dtstart", dtstart); ev.add("dtend", dtend)
    ev.add("summary", summary)
    if item.get("location"): ev.add("location", item["location"])
    if item.get("description"): ev.add("description", item["description"])
    uid = f"{hash((summary, start, end, item.get('location','')))}@dfw-influx"
    ev.add("uid", uid)
    cal.add_component(ev)

# ---------- SCRAPERS ----------

def _clean_text(x): return " ".join(x.split())

def scrape_aac(url):
    # American Airlines Center — https://www.americanairlinescenter.com/events
    out = []
    r = requests.get(url, timeout=20); r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    for card in soup.select("[data-component='EventCard']"):
        title = _clean_text(card.get_text(" ", strip=True).split("Tickets")[0])
        dt_el = card.find("time")
        when = dt_el["datetime"] if dt_el and dt_el.has_attr("datetime") else None
        if not when: continue
        # Normalize ISO date/time (site uses ISO-8601)
        start = when[:16]  # YYYY-MM-DDTHH:MM
        out.append(dict(
            summary=title,
            start=start,
            location="American Airlines Center, Dallas",
            description=url
        ))
    return out

def scrape_att_stadium(url):
    out = []
    r = requests.get(url, timeout=20); r.raise_for_status()
    s = BeautifulSoup(r.text, "lxml")
    for ev in s.select(".events-listing .event, .event-listing .event, article"):
        h = ev.find(["h2","h3"])
        d = ev.find(class_="date") or ev.find("time")
        title = _clean_text(h.get_text()) if h else None
        when = d.get_text(strip=True) if d and not d.has_attr("datetime") else d["datetime"] if d and d.has_attr("datetime") else None
        if not (title and when): continue
        # Try parse flexible date strings
        try:
            dt = datetime.strptime(when, "%B %d, %Y")
            start = dt.strftime("%Y-%m-%dT19:00")
        except Exception:
            # fallback: skip
            continue
        out.append(dict(summary=title, start=start, location="AT&T Stadium, Arlington", description=url))
    return out

def scrape_livenation_venue(url, venue_name):
    # Works for many Live Nation venue pages like Dos Equis, Echo Lounge
    out = []
    r = requests.get(url, timeout=20); r.raise_for_status()
    s = BeautifulSoup(r.text, "lxml")
    for card in s.select("[data-testid='eventCard']"):
        t = card.find(["h3","h2"])
        datebit = card.find(attrs={"data-testid":"eventDateTime"})
        title = _clean_text(t.get_text()) if t else None
        when = _clean_text(datebit.get_text()) if datebit else None
        if not (title and when): continue
        # Very rough parse: look for pattern like "Mon Sep 1, 2025 · 7:30PM"
        try:
            parts = when.replace("·","").split()
            # e.g., Mon Sep 1, 2025 7:30PM
            mon, mon_abbr, day_comma, year, timeampm = parts[:5]
            day = day_comma.strip(",")
            dt = datetime.strptime(f"{mon_abbr} {day} {year} {timeampm}", "%b %d %Y %I:%M%p")
            start = dt.strftime("%Y-%m-%dT%H:%M")
        except Exception:
            continue
        out.append(dict(summary=title, start=start, location=venue_name, description=url))
    return out

def scrape_fair_park(url):
    out = []
    r = requests.get(url, timeout=20); r.raise_for_status()
    s = BeautifulSoup(r.text, "lxml")
    for ev in s.select(".event-list .event, .grid .event, .calendar a"):
        title = ev.get_text(" ", strip=True)
        # look for data-date/aria-label/time tag
        t = ev.find("time")
        if not (title and t and t.has_attr("datetime")): continue
        start = t["datetime"][:16]
        out.append(dict(summary=title, start=start, location="Fair Park, Dallas", description=url))
    return out

def scrape_klyde(url):
    out = []
    r = requests.get(url, timeout=20); r.raise_for_status()
    s = BeautifulSoup(r.text, "lxml")
    for ev in s.select("article, .event"):
        tt = ev.find(["h2","h3"])
        tm = ev.find("time")
        title = _clean_text(tt.get_text()) if tt else None
        when = tm["datetime"] if tm and tm.has_attr("datetime") else None
        if not (title and when): continue
        start = when[:16]
        out.append(dict(summary=title, start=start, location="Klyde Warren Park, Dallas", description=url))
    return out

SCRAPERS = {
    "aac": scrape_aac,
    "att_stadium": scrape_att_stadium,
    "livenation": scrape_livenation_venue,
    "fair_park": scrape_fair_park,
    "klyde": scrape_klyde,
}

def build_from_sources(cfg, tzname):
    items_by_cat = {k: [] for k in ["sports","music","conferences","arts","festivals","specials"]}
    for src in cfg.get("sources", []):
        try:
            stype = src["type"]; url = src["url"]; cat = src["category"]
            if stype == "aac":
                items = scrape_aac(url)
            elif stype == "att_stadium":
                items = scrape_att_stadium(url)
            elif stype == "livenation":
                items = scrape_livenation_venue(url, src.get("venue","Dallas Venue"))
            elif stype == "fair_park":
                items = scrape_fair_park(url)
            elif stype == "klyde":
                items = scrape_klyde(url)
            else:
                items = []
            for it in items:
                items_by_cat.get(cat, []).append(it)
        except Exception as e:
            print(f"[warn] source failed: {src} -> {e}")
    return items_by_cat

def main():
    cfg = yaml.safe_load((ROOT / "data" / "events.yaml").read_text(encoding="utf-8"))
    tzname = cfg.get("timezone", "America/Chicago")
    cats = ["sports","music","conferences","arts","festivals","specials"]
    cals = {c: new_cal(f"DFW — {c.title()}") for c in cats}
    master = new_cal("DFW — Influx Events (Master)")

    # 1) scrape from sources
    scraped = build_from_sources(cfg, tzname)
    for cat, lst in scraped.items():
        for it in lst:
            add_event(cals[cat], it, tzname)
            add_event(master, it, tzname)

    # 2) curated YAML events
    for cat in cats:
        for it in (cfg.get("events", {}).get(cat, []) or []):
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
