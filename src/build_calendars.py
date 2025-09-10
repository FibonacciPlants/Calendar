import yaml, re, time
from pathlib import Path
from datetime import datetime, timedelta
from dateutil import tz

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from bs4 import BeautifulSoup
from icalendar import Calendar as ICal, Event as ICalEvent
from icalendar import cal as ical_mod

# ---------- config ----------
ROOT = Path(__file__).resolve().parents[1]
SITE = ROOT / "site"

TZNAME = "America/Chicago"
TZ = tz.gettz(TZNAME)

# Publish slightly in the past (small backfill) through end of FIFA window
WINDOW_START = datetime.now(TZ) - timedelta(days=7)
WINDOW_END   = datetime(2026, 6, 30, 23, 59, tzinfo=TZ)

# ---------- HTTP session with retries & UA ----------
def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/125.0 Safari/537.36 (DFW-Influx-Cal/1.0)"
    })
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"])
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s

SESSION = make_session()

def fetch(url, timeout=25):
    r = SESSION.get(url, timeout=timeout)
    r.raise_for_status()
    return r

# ---------- utils ----------
def to_local(dt):
    if dt is None: return None
    if dt.tzinfo is None: return dt.replace(tzinfo=TZ)
    return dt.astimezone(TZ)

def parse_dt_maybe_iso(s: str):
    if not s: return None
    try:
        if "T" in s:
            dt = datetime.fromisoformat(s[:16])
            return to_local(dt)
        else:
            d = datetime.fromisoformat(s)
            return to_local(d.replace(hour=0, minute=0))
    except Exception:
        return None

def text_clean(x: str) -> str:
    return re.sub(r"\s+", " ", (x or "").strip())

def new_cal(name):
    cal = ICal()
    cal.add("prodid", "-//DFW Influx Calendars//EN")
    cal.add("version", "2.0")
    cal.add("calscale", "GREGORIAN")
    cal.add("x-wr-calname", name)
    cal.add("x-wr-timezone", TZNAME)
    return cal

def add_event(cal, summary, start, end=None, location=None, description=None):
    if not start: return
    if start < WINDOW_START or start > WINDOW_END: return
    ev = ICalEvent()
    ev.add("summary", summary)
    ev.add("dtstart", start)
    ev.add("dtend", end or (start + timedelta(hours=2)))
    if location: ev.add("location", location)
    if description: ev.add("description", description)
    uid = f"{hash((summary, start.isoformat(), (end.isoformat() if end else ''), location or ''))}@dfw"
    ev.add("uid", uid)
    cal.add_component(ev)

# ---------- parsers ----------
def ics_to_items(url, fallback_location=None):
    out=[]
    r = fetch(url)
    cal = ical_mod.Calendar.from_ical(r.content)
    for comp in cal.walk():
        if comp.name != "VEVENT": continue
        summary = str(comp.get("summary", "Event"))
        dtstart = comp.get("dtstart"); dtend = comp.get("dtend")
        loc = str(comp.get("location", "")) or fallback_location
        desc= str(comp.get("description", "")) or url
        start=end=None
        if hasattr(dtstart, "dt"):
            v=dtstart.dt
            start = to_local(v if isinstance(v, datetime)
                             else datetime(v.year, v.month, v.day))
        if hasattr(dtend, "dt"):
            v=dtend.dt
            end = to_local(v if isinstance(v, datetime)
                           else datetime(v.year, v.month, v.day))
        out.append(dict(summary=summary, start=start, end=end, location=loc, description=desc))
    return out

def scrape_aac(url):
    out=[]
    s = BeautifulSoup(fetch(url).text, "lxml")
    for c in s.select("[data-component='EventCard'], article"):
        t = c.find(["h2","h3"]); tm = c.find("time")
        if not t or not tm: continue
        title = text_clean(t.get_text(" ", strip=True))
        start=None
        if tm.has_attr("datetime"):
            try:
                start = parse_dt_maybe_iso(tm["datetime"]); 
            except Exception: pass
        if title and start: out.append(dict(summary=title, start=start,
                                            location="American Airlines Center, Dallas",
                                            description=url))
    return out

def scrape_att_stadium(url):
    out=[]
    s = BeautifulSoup(fetch(url).text, "lxml")
    for ev in s.select("article, .event, .events-listing .event, .event-listing .event"):
        h = ev.find(["h2","h3"])
        if not h: continue
        title = text_clean(h.get_text(" ", strip=True))
        start=None
        tm = ev.find("time")
        if tm and tm.has_attr("datetime"):
            start = parse_dt_maybe_iso(tm["datetime"])
        if not start:
            txt = text_clean(ev.get_text(" ", strip=True))
            m = re.search(r"([A-Za-z]+ \d{1,2}, \d{4})", txt)
            if m:
                try:
                    d = datetime.strptime(m.group(1), "%B %d, %Y")
                    start = to_local(d.replace(hour=19, minute=0))
                except Exception: pass
        if title and start:
            out.append(dict(summary=title, start=start,
                            location="AT&T Stadium, Arlington", description=url))
    return out

def scrape_livenation(url, venue):
    out=[]
    s = BeautifulSoup(fetch(url).text, "lxml")
    for c in s.select("[data-testid='eventCard'], article"):
        t = c.find(["h2","h3"])
        d = c.find(attrs={"data-testid":"eventDateTime"}) or c.find("time")
        if not t or not d: continue
        title = text_clean(t.get_text())
        start=None
        if d.has_attr("datetime"):
            start = parse_dt_maybe_iso(d["datetime"])
        if not start:
            txt = text_clean(d.get_text().replace("·"," "))
            m = re.search(r"([A-Za-z]{3})\s+([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{4}).*?(\d{1,2}:\d{2}\s*[AP]M)", txt)
            if m:
                try:
                    start = datetime.strptime(
                        f"{m.group(2)} {m.group(3)} {m.group(4)} {m.group(5).upper().replace(' ','')}",
                        "%b %d %Y %I:%M%p"
                    )
                    start = to_local(start)
                except Exception: pass
        if title and start: out.append(dict(summary=title, start=start, location=venue, description=url))
    return out

def scrape_axs(url, venue):
    out=[]
    s = BeautifulSoup(fetch(url).text, "lxml")
    for card in s.select("article, .Card__Content, .event-card"):
        t = card.find(["h2","h3"]); tm = card.find("time")
        if not t or not tm or not tm.has_attr("datetime"): continue
        start = parse_dt_maybe_iso(tm["datetime"])
        if not start: continue
        out.append(dict(summary=text_clean(t.get_text()), start=start, location=venue, description=url))
    return out

def scrape_selector(url, container, title_sel, dt_sel, dt_attr=None, venue_name=None, default_time="19:00"):
    out=[]
    s = BeautifulSoup(fetch(url).text, "lxml")
    for box in s.select(container):
        t = box.select_one(title_sel)
        d = box.select_one(dt_sel)
        if not t or not d: continue
        title = text_clean(t.get_text())
        start=None
        if dt_attr and d.has_attr(dt_attr):
            start = parse_dt_maybe_iso(d[dt_attr])
        if not start:
            txt = text_clean(d.get_text())
            m = re.search(r"([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})", txt)
            if m:
                base=None
                for fmt in ("%B %d, %Y", "%b %d, %Y"):
                    try:
                        base = datetime.strptime(m.group(1), fmt); break
                    except Exception: pass
                if base:
                    hh, mm = (default_time or "19:00").split(":")
                    start = to_local(base.replace(hour=int(hh), minute=int(mm)))
        if start:
            out.append(dict(summary=title, start=start, location=venue_name or "", description=url))
    return out

# ---------- routing ----------
def fetch_from_sources(cfg):
    buckets = {k: [] for k in ["sports","music","conferences","arts","festivals","specials"]}
    for src in cfg.get("sources", []):
        stype = src.get("type"); url = src.get("url"); cat = src.get("category")
        try:
            if stype == "ics":
                items = ics_to_items(url, src.get("venue"))
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
                    default_time=src.get("default_time","19:00"),
                )
            else:
                print(f"[warn] Unknown source type: {stype} ({url})")
                items = []
        except Exception as e:
            print(f"[warn] Source failed: {url} -> {e}")
            items = []
        for it in items:
            if isinstance(it.get("start"), str):
                it["start"] = parse_dt_maybe_iso(it["start"])
            if it.get("start"):
                buckets.get(cat, []).append(it)
    return buckets

def dedupe(items):
    seen=set(); out=[]
    for it in items:
        key = (it.get("summary",""), it["start"].isoformat(), it.get("location",""))
        if key in seen: continue
        seen.add(key); out.append(it)
    return out

# ---------- main ----------
def main():
    cfg_path = ROOT / "data" / "events.yaml"
    if not cfg_path.exists():
        print("::warning:: data/events.yaml not found; creating empty config")
        cfg = {"events": {}, "sources": []}
    else:
        cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {"events": {}, "sources": []}

    cats = ["sports","music","conferences","arts","festivals","specials"]
    cals = {c: new_cal(f"DFW — {c.title()}") for c in cats}
    master = new_cal("DFW — Influx Events (Master)")

    scraped = fetch_from_sources(cfg)

    # merge curated
    for cat in cats:
        scraped.setdefault(cat, [])
        for it in (cfg.get("events", {}).get(cat, []) or []):
            if isinstance(it.get("start"), str):
                it["start"] = parse_dt_maybe_iso(it["start"])
            if it.get("start"):
                scraped[cat].append(it)

    # dedupe + add
    for cat in cats:
        for it in dedupe(scraped[cat]):
            add_event(cals[cat], it.get("summary","Event"), it["start"],
                      it.get("end"), it.get("location"), it.get("description"))
            add_event(master, it.get("summary","Event"), it["start"],
                      it.get("end"), it.get("location"), it.get("description"))

    SITE.mkdir(parents=True, exist_ok=True)
    (SITE / "sports.ics").write_bytes(cals["sports"].to_ical())
    (SITE / "music.ics").write_bytes(cals["music"].to_ical())
    (SITE / "conferences.ics").write_bytes(cals["conferences"].to_ical())
    (SITE / "arts.ics").write_bytes(cals["arts"].to_ical())
    (SITE / "festivals.ics").write_bytes(cals["festivals"].to_ical())
    (SITE / "specials_worldcup.ics").write_bytes(cals["specials"].to_ical())
    (SITE / "master.ics").write_bytes(master.to_ical())
    print("Build complete ✔")

if __name__ == "__main__":
    main()
