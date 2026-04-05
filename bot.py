"""
Alert Bot — Camden Canines
Monitors crime incidents near 1345 S Capitol St SW, Washington DC
and posts alerts to Discord. Also provides:
  - Weekly event digest (Sundays at 7 PM ET)
  - Day-of event reminders (8 AM ET)
  - Ad hoc alerts for last-minute event additions

Venues monitored:
  - Nationals Park (MLB games, fireworks, concerts)
  - Audi Field (DC United, events)
  - Capital One Arena (Wizards, Capitals, concerts)
  - The Anthem (concerts, shows)
  - National Mall (NPS events, annual fireworks)
"""

import os
import json
import sqlite3
import logging
import asyncio
import math
import requests
from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo

import discord
from discord.ext import tasks
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
DISCORD_CHANNEL_ID = int(os.environ["DISCORD_CHANNEL_ID"])

# 1345 S Capitol St SW, Washington DC
TARGET_LAT = 38.8726
TARGET_LON = -77.0105
RADIUS_M = 400  # metres

DB_PATH = "alerts.db"
ET = ZoneInfo("America/New_York")

# DC Open Data – Crime Incidents (Socrata)
CRIME_API = "https://data.dc.gov/resource/jwta-jx6e.json"
CRIME_APP_TOKEN = ""  # optional – set SOCRATA_APP_TOKEN env var if throttled

# MLB Stats API — teamId 120 = Washington Nationals
MLB_STATS_API = (
    "https://statsapi.mlb.com/api/v1/schedule"
    "?lang=en&sportIds=1&hydrate=game(promotions)"
    "&teamId=120&timeZone=America/New_York&scheduleTypes=games"
)

# NPS Events API — parkCode 'nama' = National Mall & Memorial Parks
NPS_API_KEY = os.environ.get("NPS_API_KEY", "")
NPS_EVENTS_API = "https://developer.nps.gov/api/v1/events?parkCode=nama&limit=50"

# Ticketmaster Discovery API
TICKETMASTER_API_KEY = os.environ.get("TICKETMASTER_API_KEY", "")
TM_EVENTS_API = "https://app.ticketmaster.com/discovery/v2/events.json"

# Search radius in miles around the building for Ticketmaster events.
# 3 miles covers: Navy Yard, Nationals Park, Audi Field, The Anthem,
# Capital One Arena — and any other local event automatically.
TM_RADIUS_MILES = 3

# Known annual fireworks events near 1345 S Capitol St SW.
ANNUAL_FIREWORKS = [
    (7, 4,  "Independence Day — National Mall",
     "The National Mall fireworks display is one of the largest in the country. "
     "Expect **road closures, Metro crowding, and loud noise** for several hours."),
    (12, 31, "New Year's Eve — Washington Monument",
     "Fireworks and light show at the Washington Monument at midnight. "
     "Expect **road closures and heavy traffic** near the Mall."),
]

# Crime-type colour coding
VIOLENT_OFFENSES = {
    "HOMICIDE", "SEX ABUSE", "ASSAULT W/DANGEROUS WEAPON", "ROBBERY",
}
PROPERTY_OFFENSES = {
    "BURGLARY", "THEFT/OTHER", "THEFT F/AUTO", "MOTOR VEHICLE THEFT",
    "ARSON",
}


# ── Database ──────────────────────────────────────────────────────────────────
def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sent_alerts (
            id TEXT PRIMARY KEY,
            alerted_at TEXT NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fireworks_alerts (
            event_date TEXT PRIMARY KEY,
            alerted_at TEXT NOT NULL
        )
    """)
    # Track weekly digest contents so we can detect ad-hoc additions
    cur.execute("""
        CREATE TABLE IF NOT EXISTS digest_events (
            digest_date TEXT NOT NULL,
            event_key TEXT NOT NULL,
            PRIMARY KEY (digest_date, event_key)
        )
    """)
    # Track day-of reminders so we don't double-post
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dayof_reminders (
            event_key TEXT PRIMARY KEY,
            reminded_at TEXT NOT NULL
        )
    """)
    con.commit()
    con.close()
    log.info("Database initialised at %s", DB_PATH)


def already_sent(incident_id: str) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT 1 FROM sent_alerts WHERE id = ?", (incident_id,))
    found = cur.fetchone() is not None
    con.close()
    return found


def mark_sent(incident_id: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO sent_alerts (id, alerted_at) VALUES (?, ?)",
        (incident_id, datetime.now(timezone.utc).isoformat()),
    )
    con.commit()
    con.close()


def fireworks_already_alerted(event_date: str) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "SELECT 1 FROM fireworks_alerts WHERE event_date = ?", (event_date,)
    )
    found = cur.fetchone() is not None
    con.close()
    return found


def mark_fireworks_alerted(event_date: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO fireworks_alerts (event_date, alerted_at) VALUES (?, ?)",
        (event_date, datetime.now(timezone.utc).isoformat()),
    )
    con.commit()
    con.close()


def save_digest_events(digest_date: str, event_keys: list[str]):
    """Save which event keys were included in a weekly digest."""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    for key in event_keys:
        cur.execute(
            "INSERT OR IGNORE INTO digest_events (digest_date, event_key) VALUES (?, ?)",
            (digest_date, key),
        )
    con.commit()
    con.close()


def get_last_digest_event_keys() -> set[str]:
    """Get event keys from the most recent digest."""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT DISTINCT event_key FROM digest_events ORDER BY digest_date DESC")
    keys = {row[0] for row in cur.fetchall()}
    con.close()
    return keys


def dayof_already_reminded(event_key: str) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT 1 FROM dayof_reminders WHERE event_key = ?", (event_key,))
    found = cur.fetchone() is not None
    con.close()
    return found


def mark_dayof_reminded(event_key: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO dayof_reminders (event_key, reminded_at) VALUES (?, ?)",
        (event_key, datetime.now(timezone.utc).isoformat()),
    )
    con.commit()
    con.close()


# ── Geo helpers ───────────────────────────────────────────────────────────────
def haversine_metres(lat1, lon1, lat2, lon2) -> float:
    """Return distance in metres between two lat/lon points."""
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


# ── Crime API ─────────────────────────────────────────────────────────────────
def fetch_recent_crimes() -> list[dict]:
    since = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S")
    params = {
        "$where": f"report_dat >= '{since}'",
        "$limit": 1000,
        "$order": "report_dat DESC",
    }
    headers = {}
    app_token = os.environ.get("SOCRATA_APP_TOKEN", CRIME_APP_TOKEN)
    if app_token:
        headers["X-App-Token"] = app_token

    try:
        resp = requests.get(CRIME_API, params=params, headers=headers, timeout=20)
        resp.raise_for_status()
        records = resp.json()
    except Exception as exc:
        log.error("Crime API error: %s", exc)
        return []

    nearby = []
    for r in records:
        try:
            lat = float(r.get("latitude") or r.get("lat") or 0)
            lon = float(r.get("longitude") or r.get("lon") or 0)
        except (TypeError, ValueError):
            continue
        if lat == 0 and lon == 0:
            continue
        dist = haversine_metres(TARGET_LAT, TARGET_LON, lat, lon)
        if dist <= RADIUS_M:
            r["_distance_m"] = round(dist)
            nearby.append(r)

    log.info("Found %d crime(s) within %dm in the last 24 h", len(nearby), RADIUS_M)
    return nearby


def embed_for_crime(record: dict) -> discord.Embed:
    offense = (record.get("offense") or record.get("offensegroup") or "UNKNOWN").upper()
    method = record.get("method") or ""
    block = record.get("block") or record.get("blocksiteaddress") or "Unknown location"
    report_dt_raw = record.get("report_dat") or record.get("reportdatetime") or ""
    district = record.get("district") or record.get("psa") or "N/A"
    ccn = record.get("ccn") or "N/A"
    dist_m = record.get("_distance_m", "?")
    shift = record.get("shift") or ""

    try:
        dt = datetime.fromisoformat(report_dt_raw.replace("Z", "+00:00"))
        time_str = dt.strftime("%b %d, %Y %I:%M %p UTC")
    except Exception:
        time_str = report_dt_raw or "Unknown time"

    if offense in VIOLENT_OFFENSES:
        colour = discord.Colour.red()
        severity = "🔴 VIOLENT"
    elif offense in PROPERTY_OFFENSES:
        colour = discord.Colour.orange()
        severity = "🟠 PROPERTY"
    else:
        colour = discord.Colour.yellow()
        severity = "🟡 OTHER"

    title = f"{severity} CRIME — {offense}"
    if method:
        title += f" ({method})"

    embed = discord.Embed(
        title=title,
        description=f"📍 **{block}**\n_{dist_m}m from 1345 S Capitol St SW_",
        colour=colour,
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="Reported", value=time_str, inline=True)
    embed.add_field(name="District/PSA", value=str(district), inline=True)
    if shift:
        embed.add_field(name="Shift", value=shift, inline=True)
    embed.set_footer(text=f"CCN: {ccn} • DC MPD Crime Data")
    return embed


# ── Event fetching helpers ───────────────────────────────────────────────────
def make_event_key(event: dict) -> str:
    """Stable key for dedup: date + venue + name."""
    return f"{event['date']}|{event['venue']}|{event['name']}"


def fetch_mlb_games_and_fireworks(start: date, end: date) -> list[dict]:
    """Nationals games (with fireworks flagging) from MLB Stats API."""
    url = f"{MLB_STATS_API}&season={start.year}&startDate={start}&endDate={end}"
    try:
        resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0 AlertBot/2.0"})
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        log.warning("MLB Stats API error: %s", exc)
        return []

    results = []
    for date_obj in data.get("dates", []):
        for game in date_obj.get("games", []):
            game_date = date_obj["date"]
            away = game.get("teams", {}).get("away", {}).get("team", {}).get("name", "TBD")
            home = game.get("teams", {}).get("home", {}).get("team", {}).get("name", "Nationals")
            game_time = ""
            try:
                gdt = datetime.fromisoformat(game.get("gameDate", "").replace("Z", "+00:00"))
                game_time = gdt.astimezone(ET).strftime("%I:%M %p")
            except Exception:
                pass

            has_fireworks = False
            for promo in game.get("promotions", []):
                if "firework" in json.dumps(promo).lower():
                    has_fireworks = True
                    break

            name = f"{away} @ {home}"
            if has_fireworks:
                name += " + 🎆 Fireworks"

            results.append({
                "date": game_date,
                "name": name,
                "venue": "Nationals Park",
                "emoji": "⚾",
                "time": game_time,
                "has_fireworks": has_fireworks,
                "source": "MLB",
                "description": (
                    "Nationals game at Nationals Park."
                    + (" **Fireworks after the game!**" if has_fireworks else "")
                    + " Expect traffic near 1345 S Capitol St SW."
                ),
            })
    log.info("MLB: found %d game(s) between %s and %s", len(results), start, end)
    return results


def fetch_ticketmaster_events(start: date, end: date) -> list[dict]:
    """Events from Ticketmaster Discovery API for monitored venues."""
    if not TICKETMASTER_API_KEY:
        log.info("No TICKETMASTER_API_KEY — skipping Ticketmaster")
        return []

    # Single radius-based search — returns all ticketed events within TM_RADIUS_MILES
    # of the building. Automatically covers Nationals Park, Audi Field, The Anthem,
    # Capital One Arena, Navy Yard, and any other local venue without needing to
    # name them explicitly.
    params = {
        "apikey": TICKETMASTER_API_KEY,
        "latlong": f"{TARGET_LAT},{TARGET_LON}",
        "radius": str(TM_RADIUS_MILES),
        "unit": "miles",
        "startDateTime": f"{start}T00:00:00Z",
        "endDateTime": f"{end}T23:59:59Z",
        "size": 100,
        "sort": "date,asc",
    }
    try:
        resp = requests.get(TM_EVENTS_API, params=params, timeout=20,
                            headers={"User-Agent": "Mozilla/5.0 AlertBot/2.0"})
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        log.warning("Ticketmaster radius search error: %s", exc)
        return []

    # Emoji by segment type
    segment_emoji = {
        "sports": "🏟️",
        "music": "🎵",
        "arts & theatre": "🎭",
        "film": "🎬",
        "miscellaneous": "🎪",
    }

    results = []
    for event in data.get("_embedded", {}).get("events", []):
        dates_info = event.get("dates", {}).get("start", {})
        event_date = dates_info.get("localDate", "")
        if not event_date:
            continue

        local_time = dates_info.get("localTime", "")
        event_time = ""
        if local_time:
            try:
                t = datetime.strptime(local_time, "%H:%M:%S")
                event_time = t.strftime("%I:%M %p")
            except Exception:
                event_time = local_time

        event_name = event.get("name", "Event")

        # Get venue name from the event
        event_venues = event.get("_embedded", {}).get("venues", [{}])
        venue_name = event_venues[0].get("name", "Local Venue") if event_venues else "Local Venue"

        # Pick emoji based on event segment
        segment = ""
        try:
            segment = event.get("classifications", [{}])[0].get("segment", {}).get("name", "").lower()
        except (IndexError, KeyError):
            pass
        emoji = segment_emoji.get(segment, "📍")

        has_fireworks = "firework" in event_name.lower()

        results.append({
            "date": event_date,
            "name": event_name,
            "venue": venue_name,
            "emoji": emoji,
            "time": event_time,
            "has_fireworks": has_fireworks,
            "source": "Ticketmaster",
            "description": f"{event_name} at {venue_name}.",
        })

    log.info("Ticketmaster radius search: %d event(s) within %d miles", len(results), TM_RADIUS_MILES)
    return results


def fetch_annual_events(start: date, end: date) -> list[dict]:
    """Known annual fireworks events within the date range."""
    results = []
    for month, day, name, description in ANNUAL_FIREWORKS:
        for year in (start.year, start.year + 1):
            try:
                event_date = date(year, month, day)
            except ValueError:
                continue
            if start <= event_date <= end:
                results.append({
                    "date": str(event_date),
                    "name": name,
                    "venue": "National Mall",
                    "emoji": "🎆",
                    "time": "",
                    "has_fireworks": True,
                    "source": "Annual Event",
                    "description": description,
                })
    return results


def fetch_nps_events(start: date, end: date) -> list[dict]:
    """NPS scheduled events at the National Mall."""
    if not NPS_API_KEY:
        log.info("No NPS_API_KEY — skipping NPS events")
        return []

    url = f"{NPS_EVENTS_API}&api_key={NPS_API_KEY}&dateStart={start}&dateEnd={end}"
    try:
        resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0 AlertBot/2.0"})
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        log.warning("NPS Events API error: %s", exc)
        return []

    results = []
    for event in data.get("data", []):
        title = event.get("title", "")
        desc = event.get("description", "")
        date_str = (event.get("dates") or [{}])[0].get("date", "")
        if not date_str:
            continue

        has_fireworks = "firework" in (title + desc).lower()

        results.append({
            "date": date_str[:10],
            "name": title or "National Mall Event",
            "venue": "National Mall",
            "emoji": "🎆" if has_fireworks else "🏛️",
            "time": "",
            "has_fireworks": has_fireworks,
            "source": "NPS",
            "description": desc[:200] if desc else f"{title} at the National Mall.",
        })
    log.info("NPS: found %d event(s)", len(results))
    return results


def fetch_all_events(start: date, end: date) -> list[dict]:
    """Gather events from all sources for a date range, deduped."""
    all_events = (
        fetch_mlb_games_and_fireworks(start, end)
        + fetch_ticketmaster_events(start, end)
        + fetch_annual_events(start, end)
        + fetch_nps_events(start, end)
    )

    # Deduplicate by key
    seen = set()
    unique = []
    for e in all_events:
        key = make_event_key(e)
        if key in seen:
            continue
        seen.add(key)
        unique.append(e)

    # Sort by date, then time
    unique.sort(key=lambda e: (e["date"], e.get("time", "")))
    return unique


# ── Embed builders ───────────────────────────────────────────────────────────
def build_weekly_digest_embed(events: list[dict], week_start: date) -> list[discord.Embed]:
    """Build embed(s) for the weekly digest. Returns a list since Discord limits embed size."""
    week_end = week_start + timedelta(days=6)
    title = f"📅 Weekly Events — {week_start.strftime('%b %d')} to {week_end.strftime('%b %d, %Y')}"

    if not events:
        embed = discord.Embed(
            title=title,
            description="No events scheduled this week. Enjoy the quiet! 🤫",
            colour=discord.Colour.light_grey(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.set_footer(text="Alert Bot • Weekly Digest")
        return [embed]

    # Group events by date
    by_date: dict[str, list[dict]] = {}
    for e in events:
        by_date.setdefault(e["date"], []).append(e)

    lines = []
    for event_date in sorted(by_date.keys()):
        try:
            d = datetime.strptime(event_date, "%Y-%m-%d").date()
            day_label = d.strftime("%A, %b %d")
        except ValueError:
            day_label = event_date

        lines.append(f"\n**{day_label}**")
        for e in by_date[event_date]:
            time_str = f" at {e['time']}" if e.get("time") else ""
            fireworks_flag = " 🎆" if e.get("has_fireworks") else ""
            lines.append(f"{e['emoji']} {e['name']}{time_str} — _{e['venue']}_{fireworks_flag}")

    description = "\n".join(lines)

    # Discord embed description limit is 4096 chars — split if needed
    embeds = []
    if len(description) <= 4000:
        embed = discord.Embed(
            title=title,
            description=description,
            colour=discord.Colour.blue(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.set_footer(text=f"Alert Bot • {len(events)} event(s) this week")
        embeds.append(embed)
    else:
        # Split into multiple embeds
        chunk_size = 3800
        chunks = []
        current = ""
        for line in lines:
            if len(current) + len(line) + 1 > chunk_size:
                chunks.append(current)
                current = line
            else:
                current += "\n" + line if current else line
        if current:
            chunks.append(current)

        for i, chunk in enumerate(chunks):
            embed = discord.Embed(
                title=title if i == 0 else f"{title} (cont.)",
                description=chunk,
                colour=discord.Colour.blue(),
                timestamp=datetime.now(timezone.utc),
            )
            if i == len(chunks) - 1:
                embed.set_footer(text=f"Alert Bot • {len(events)} event(s) this week")
            embeds.append(embed)

    return embeds


def build_dayof_embed(events: list[dict], today: date) -> discord.Embed:
    """Build embed for day-of reminders."""
    day_label = today.strftime("%A, %B %d")
    title = f"🔔 Today's Events — {day_label}"

    lines = []
    for e in events:
        time_str = f" at {e['time']}" if e.get("time") else ""
        fireworks_flag = " 🎆" if e.get("has_fireworks") else ""
        lines.append(f"{e['emoji']} **{e['name']}**{time_str} — _{e['venue']}_{fireworks_flag}")

    description = "\n".join(lines)
    if any(e.get("has_fireworks") for e in events):
        description += "\n\n🎆 **Fireworks tonight!** Plan for noise and traffic near the building."
    else:
        description += "\n\nPlan for possible traffic and noise near the building."

    embed = discord.Embed(
        title=title,
        description=description,
        colour=discord.Colour.gold(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_footer(text=f"Alert Bot • {len(events)} event(s) today")
    return embed


def build_adhoc_embed(event: dict) -> discord.Embed:
    """Build embed for an ad-hoc new event alert."""
    time_str = f" at {event['time']}" if event.get("time") else ""
    fireworks_flag = " 🎆" if event.get("has_fireworks") else ""
    try:
        d = datetime.strptime(event["date"], "%Y-%m-%d").date()
        date_label = d.strftime("%A, %b %d")
    except ValueError:
        date_label = event["date"]

    embed = discord.Embed(
        title=f"🆕 New Event Added — {event['name']}",
        description=(
            f"{event['emoji']} **{event['name']}**{time_str}{fireworks_flag}\n"
            f"📍 {event['venue']} — {date_label}\n\n"
            f"{event.get('description', '')}\n\n"
            f"_This event was added after the last weekly digest._"
        ),
        colour=discord.Colour.purple(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_footer(text=f"Alert Bot • Ad Hoc Alert • Source: {event['source']}")
    return embed


def embed_for_fireworks(event: dict) -> discord.Embed:
    """Legacy fireworks embed for the !test_fireworks command."""
    is_today = event.get("is_today", False)
    when = "**TONIGHT**" if is_today else f"**TOMORROW** ({event['date']})"
    embed = discord.Embed(
        title=f"🎆 {event['name']}",
        description=f"Fireworks are scheduled {when}.\n\n{event['description']}\n\nPlan accordingly!",
        colour=discord.Colour.blue(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_footer(text=f"Source: {event['source']}")
    return embed


# ── Discord bot ───────────────────────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.presences = True

bot = discord.Client(intents=intents)
scheduler = AsyncIOScheduler(timezone="America/New_York")


async def check_crimes():
    log.info("Running crime check…")
    channel = bot.get_channel(DISCORD_CHANNEL_ID)
    if channel is None:
        log.error("Cannot find channel %d — check DISCORD_CHANNEL_ID", DISCORD_CHANNEL_ID)
        return

    crimes = fetch_recent_crimes()
    new_count = 0
    for crime in crimes:
        ccn = crime.get("ccn") or ""
        report_dt = crime.get("report_dat") or crime.get("reportdatetime") or ""
        block = crime.get("block") or crime.get("blocksiteaddress") or ""
        offense = crime.get("offense") or ""
        uid = ccn if ccn else f"{report_dt}|{block}|{offense}"

        if already_sent(uid):
            continue

        embed = embed_for_crime(crime)
        try:
            await channel.send(embed=embed)
            mark_sent(uid)
            new_count += 1
            await asyncio.sleep(0.5)
        except discord.DiscordException as exc:
            log.error("Failed to send crime embed: %s", exc)

    if new_count:
        log.info("Posted %d new crime alert(s)", new_count)
    else:
        log.info("No new incidents to post")


async def weekly_digest():
    """Sunday 7 PM ET — post the week's event calendar."""
    log.info("Running weekly digest…")
    channel = bot.get_channel(DISCORD_CHANNEL_ID)
    if channel is None:
        return

    now_et = datetime.now(ET)
    # Next Monday through Sunday
    week_start = (now_et + timedelta(days=1)).date()
    week_end = week_start + timedelta(days=6)

    events = fetch_all_events(week_start, week_end)
    embeds = build_weekly_digest_embed(events, week_start)

    # Save digest event keys for ad-hoc dedup
    digest_date = str(now_et.date())
    event_keys = [make_event_key(e) for e in events]
    save_digest_events(digest_date, event_keys)

    try:
        for embed in embeds:
            await channel.send(embed=embed)
            await asyncio.sleep(0.5)
        log.info("Posted weekly digest with %d event(s)", len(events))
    except discord.DiscordException as exc:
        log.error("Failed to send weekly digest: %s", exc)


async def dayof_reminder():
    """8 AM ET daily — remind about today's events."""
    log.info("Running day-of reminder check…")
    channel = bot.get_channel(DISCORD_CHANNEL_ID)
    if channel is None:
        return

    today = datetime.now(ET).date()
    events = fetch_all_events(today, today)

    if not events:
        log.info("No events today — skipping day-of reminder")
        return

    # Only remind for events not yet reminded
    new_events = []
    for e in events:
        key = make_event_key(e)
        if not dayof_already_reminded(key):
            new_events.append(e)
            mark_dayof_reminded(key)

    if not new_events:
        log.info("All today's events already reminded")
        return

    embed = build_dayof_embed(new_events, today)
    try:
        await channel.send(embed=embed)
        log.info("Posted day-of reminder for %d event(s)", len(new_events))
    except discord.DiscordException as exc:
        log.error("Failed to send day-of reminder: %s", exc)


async def adhoc_check():
    """Every 6 hours — check for newly added events not in the last digest."""
    log.info("Running ad-hoc event check…")
    channel = bot.get_channel(DISCORD_CHANNEL_ID)
    if channel is None:
        return

    now_et = datetime.now(ET)
    today = now_et.date()
    # Look ahead through the end of the current digest week (next Sunday)
    days_until_sunday = (6 - today.weekday()) % 7
    if days_until_sunday == 0:
        days_until_sunday = 7
    end = today + timedelta(days=days_until_sunday)

    events = fetch_all_events(today, end)
    digest_keys = get_last_digest_event_keys()

    if not digest_keys:
        # No digest has been sent yet — skip ad-hoc
        log.info("No previous digest found — skipping ad-hoc check")
        return

    new_events = []
    for e in events:
        key = make_event_key(e)
        if key not in digest_keys:
            # Check it wasn't already ad-hoc alerted (reuse fireworks_alerts table)
            adhoc_key = f"adhoc_{key}"
            if not fireworks_already_alerted(adhoc_key):
                new_events.append(e)
                mark_fireworks_alerted(adhoc_key)

    if not new_events:
        log.info("No new ad-hoc events found")
        return

    for event in new_events:
        try:
            await channel.send(embed=build_adhoc_embed(event))
            log.info("Posted ad-hoc alert: %s on %s", event["name"], event["date"])
            await asyncio.sleep(0.5)
        except discord.DiscordException as exc:
            log.error("Failed to send ad-hoc alert: %s", exc)


@bot.event
async def on_ready():
    log.info("Logged in as %s (ID: %s)", bot.user, bot.user.id)

    # Crime check — every 15 minutes
    scheduler.add_job(check_crimes, "interval", minutes=15, id="crime_check",
                      next_run_time=datetime.now(timezone.utc))

    # Weekly digest — Sundays at 7 PM ET
    scheduler.add_job(weekly_digest, "cron", day_of_week="sun", hour=19, minute=0,
                      id="weekly_digest")

    # Day-of reminder — 8 AM ET every day
    scheduler.add_job(dayof_reminder, "cron", hour=8, minute=0,
                      id="dayof_reminder")

    # Ad-hoc check — every 6 hours (catches new events between digests)
    scheduler.add_job(adhoc_check, "interval", hours=6, id="adhoc_check",
                      next_run_time=datetime.now(timezone.utc) + timedelta(minutes=5))

    scheduler.start()
    log.info("Scheduler started — crime:15m, digest:Sun 7pm, dayof:8am, adhoc:6h")

    channel = bot.get_channel(DISCORD_CHANNEL_ID)
    if channel:
        try:
            await channel.send(
                embed=discord.Embed(
                    title="✅ Alert Bot is online",
                    description=(
                        "Monitoring **crime** within 400m of 1345 S Capitol St SW (every 15 min)\n"
                        "📅 **Weekly digest** Sundays at 7 PM\n"
                        "🔔 **Day-of reminders** at 8 AM\n"
                        "🆕 **Ad-hoc alerts** for last-minute additions\n\n"
                        "Venues: Nationals Park, Audi Field, Capital One Arena, The Anthem, National Mall"
                    ),
                    colour=discord.Colour.green(),
                    timestamp=datetime.now(timezone.utc),
                )
            )
        except discord.DiscordException as exc:
            log.error("Could not send startup message: %s", exc)


@bot.event
async def on_message(message: discord.Message):
    """Handle test commands."""
    if message.author == bot.user:
        return

    content = message.content.strip().lower()

    if content == "!test_fireworks":
        log.info("!test_fireworks triggered by %s", message.author)
        await message.channel.send("🔍 Checking all fireworks sources…")

        today = datetime.now(ET).date()
        all_events = (
            fetch_mlb_games_and_fireworks(today, today + timedelta(days=90))
            + fetch_annual_events(today, today + timedelta(days=365))
            + fetch_nps_events(today, today + timedelta(days=30))
        )
        fireworks_events = [e for e in all_events if e.get("has_fireworks")]

        seen = set()
        unique = []
        for e in fireworks_events:
            key = make_event_key(e)
            if key not in seen:
                seen.add(key)
                unique.append(e)
        unique.sort(key=lambda x: x["date"])

        if not unique:
            await message.channel.send("No upcoming fireworks events found across any source.")
        else:
            summary = "\n".join(
                f"• **{e['date']}** — {e['name']} _(via {e['source']})_"
                for e in unique[:8]
            )
            await message.channel.send(
                f"Found **{len(unique)}** upcoming fireworks event(s):\n{summary}"
            )

    elif content == "!test_digest":
        log.info("!test_digest triggered by %s", message.author)
        await message.channel.send("📅 Generating test weekly digest…")

        now_et = datetime.now(ET)
        week_start = (now_et + timedelta(days=1)).date()
        week_end = week_start + timedelta(days=6)
        events = fetch_all_events(week_start, week_end)

        embeds = build_weekly_digest_embed(events, week_start)
        for embed in embeds:
            await message.channel.send(embed=embed)
            await asyncio.sleep(0.5)
        await message.channel.send(f"_(Test only — {len(events)} event(s) found, not saved to digest)_")

    elif content == "!test_today":
        log.info("!test_today triggered by %s", message.author)
        await message.channel.send("🔔 Checking today's events…")

        today = datetime.now(ET).date()
        events = fetch_all_events(today, today)

        if not events:
            await message.channel.send("No events scheduled today.")
        else:
            embed = build_dayof_embed(events, today)
            await message.channel.send(embed=embed)
            await message.channel.send(f"_(Test only — {len(events)} event(s) found)_")

    elif content == "!help":
        help_embed = discord.Embed(
            title="🤖 Alert Bot Commands",
            description=(
                "**!test_fireworks** — Show upcoming fireworks events\n"
                "**!test_digest** — Preview next week's event digest\n"
                "**!test_today** — Show today's events\n"
                "**!help** — Show this help message"
            ),
            colour=discord.Colour.blurple(),
        )
        await message.channel.send(embed=help_embed)


@bot.event
async def on_error(event, *args, **kwargs):
    log.exception("Unhandled error in event %s", event)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    init_db()
    log.info("Starting Alert Bot…")
    bot.run(DISCORD_TOKEN, log_handler=None)
