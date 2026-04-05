"""
DC Safety Bot
Monitors crime incidents near 1345 S Capitol St SW, Washington DC
and posts alerts to a Discord channel every 15 minutes.
Also monitors for fireworks events near the building:
  - Nationals Park game promotions (MLB Stats API)
  - National Mall annual events (July 4, New Year's Eve)
  - NPS scheduled events on the National Mall (NPS Events API)
"""

import os
import json
import sqlite3
import logging
import asyncio
import math
import requests
from datetime import datetime, timedelta, timezone

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
# Free key via https://www.nps.gov/subjects/developer/get-started.htm
NPS_API_KEY = os.environ.get("NPS_API_KEY", "")
NPS_EVENTS_API = "https://developer.nps.gov/api/v1/events?parkCode=nama&limit=50"

# Known annual fireworks events near 1345 S Capitol St SW.
# Each entry is (month, day, name, description).
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
        (incident_id, datetime.utcnow().isoformat()),
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
        (event_date, datetime.utcnow().isoformat()),
    )
    con.commit()
    con.close()


# ── Geo helpers ───────────────────────────────────────────────────────────────
def haversine_metres(lat1, lon1, lat2, lon2) -> float:
    """Return distance in metres between two lat/lon points."""
    R = 6_371_000  # Earth radius in metres
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


# ── Crime API ─────────────────────────────────────────────────────────────────
def fetch_recent_crimes() -> list[dict]:
    """
    Pull crime incidents from DC Open Data for the last 24 h
    and filter to those within RADIUS_M of TARGET_LAT/LON.
    """
    since = (datetime.utcnow() - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S")
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

    # Parse timestamp
    try:
        dt = datetime.fromisoformat(report_dt_raw.replace("Z", "+00:00"))
        time_str = dt.strftime("%b %d, %Y %I:%M %p UTC")
    except Exception:
        time_str = report_dt_raw or "Unknown time"

    # Colour by severity
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


# ── Fireworks check ───────────────────────────────────────────────────────────
def fetch_mlb_fireworks_dates() -> list[dict]:
    """Nationals game promotions that mention fireworks (MLB Stats API)."""
    today = datetime.utcnow().date()
    year = today.year
    url = f"{MLB_STATS_API}&season={year}&startDate={today}&endDate={year}-12-31"
    try:
        resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0 DC-Safety-Bot/1.0"})
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        log.warning("MLB Stats API error: %s", exc)
        return []

    results = []
    for date_obj in data.get("dates", []):
        for game in date_obj.get("games", []):
            for promo in game.get("promotions", []):
                if "firework" in json.dumps(promo).lower():
                    results.append({
                        "date": date_obj["date"],
                        "name": promo.get("name", "Fireworks Night"),
                        "source": "Nationals Park",
                        "description": (
                            "Fireworks are scheduled after tonight's Nationals game. "
                            "Expect **heavy traffic and noise** near 1345 S Capitol St SW."
                        ),
                    })
                    break
    log.info("MLB fireworks dates: %s", [r["date"] for r in results])
    return results


def fetch_annual_fireworks_dates() -> list[dict]:
    """Known annual fireworks events — July 4th and New Year's Eve."""
    today = datetime.utcnow().date()
    results = []
    for month, day, name, description in ANNUAL_FIREWORKS:
        try:
            event_date = today.replace(month=month, day=day)
        except ValueError:
            continue
        results.append({
            "date": str(event_date),
            "name": name,
            "source": "Annual Event",
            "description": description,
        })
    log.info("Annual fireworks dates: %s", [r["date"] for r in results])
    return results


def fetch_nps_fireworks_dates() -> list[dict]:
    """NPS events at the National Mall that mention fireworks."""
    if not NPS_API_KEY:
        log.info("No NPS_API_KEY set — skipping NPS events check")
        return []

    today = datetime.utcnow().date()
    url = f"{NPS_EVENTS_API}&api_key={NPS_API_KEY}&dateStart={today}&dateEnd={today + timedelta(days=30)}&q=fireworks"
    try:
        resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0 DC-Safety-Bot/1.0"})
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        log.warning("NPS Events API error: %s", exc)
        return []

    results = []
    for event in data.get("data", []):
        title = event.get("title", "")
        desc = event.get("description", "")
        if "firework" not in (title + desc).lower():
            continue
        date_str = (event.get("dates") or [{}])[0].get("date", "")
        if not date_str:
            continue
        results.append({
            "date": date_str[:10],
            "name": title or "National Mall Fireworks",
            "source": "NPS / National Mall",
            "description": (
                "A fireworks event is scheduled at the National Mall. "
                "Expect **road closures and heavy traffic** near 1345 S Capitol St SW."
            ),
        })
    log.info("NPS fireworks dates: %s", [r["date"] for r in results])
    return results


def fetch_all_fireworks() -> list[dict]:
    """Combine all fireworks sources and return upcoming dates (today + tomorrow)."""
    today = datetime.utcnow().date()
    tomorrow = today + timedelta(days=1)
    relevant = []

    all_events = (
        fetch_mlb_fireworks_dates()
        + fetch_annual_fireworks_dates()
        + fetch_nps_fireworks_dates()
    )

    seen = set()
    for event in all_events:
        try:
            d = datetime.strptime(event["date"], "%Y-%m-%d").date()
        except ValueError:
            continue
        if d not in (today, tomorrow):
            continue
        key = (event["date"], event["name"])
        if key in seen:
            continue
        seen.add(key)
        event["is_today"] = d == today
        relevant.append(event)

    return relevant


def embed_for_fireworks(event: dict) -> discord.Embed:
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
scheduler = AsyncIOScheduler(timezone="UTC")


async def check_crimes():
    log.info("Running crime check…")
    channel = bot.get_channel(DISCORD_CHANNEL_ID)
    if channel is None:
        log.error("Cannot find channel %d — check DISCORD_CHANNEL_ID", DISCORD_CHANNEL_ID)
        return

    crimes = fetch_recent_crimes()
    new_count = 0
    for crime in crimes:
        # Build a stable unique key
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
            await asyncio.sleep(0.5)  # avoid rate-limit bursts
        except discord.DiscordException as exc:
            log.error("Failed to send crime embed: %s", exc)

    if new_count:
        log.info("Posted %d new crime alert(s)", new_count)
    else:
        log.info("No new incidents to post")


async def check_fireworks():
    log.info("Running fireworks check…")
    channel = bot.get_channel(DISCORD_CHANNEL_ID)
    if channel is None:
        return

    events = fetch_all_fireworks()
    for event in events:
        alert_key = f"{event['date']}_{event['name']}_{'today' if event['is_today'] else 'tomorrow'}"
        if fireworks_already_alerted(alert_key):
            continue

        try:
            await channel.send(embed=embed_for_fireworks(event))
            mark_fireworks_alerted(alert_key)
            log.info("Posted fireworks alert: %s on %s", event['name'], event['date'])
        except discord.DiscordException as exc:
            log.error("Failed to send fireworks embed: %s", exc)


@bot.event
async def on_ready():
    log.info("Logged in as %s (ID: %s)", bot.user, bot.user.id)

    # Schedule jobs
    scheduler.add_job(check_crimes, "interval", minutes=15, id="crime_check",
                      next_run_time=datetime.now(timezone.utc))
    scheduler.add_job(check_fireworks, "interval", hours=6, id="fireworks_check",
                      next_run_time=datetime.now(timezone.utc))
    scheduler.start()
    log.info("Scheduler started — crime check every 15 min, fireworks every 6 h")

    channel = bot.get_channel(DISCORD_CHANNEL_ID)
    if channel:
        try:
            await channel.send(
                embed=discord.Embed(
                    title="✅ DC Safety Bot is online",
                    description=(
                        "Monitoring crime incidents within **400m of 1345 S Capitol St SW**.\n"
                        "Checks run every **15 minutes**. Fireworks alerts also enabled."
                    ),
                    colour=discord.Colour.green(),
                    timestamp=datetime.now(timezone.utc),
                )
            )
        except discord.DiscordException as exc:
            log.error("Could not send startup message: %s", exc)


@bot.event
async def on_message(message: discord.Message):
    """Handle !test_fireworks command for manual testing."""
    if message.author == bot.user:
        return
    if message.content.strip().lower() != "!test_fireworks":
        return

    log.info("!test_fireworks triggered by %s", message.author)
    await message.channel.send("🔍 Checking all fireworks sources (Nationals, National Mall, annual events)…")

    # For testing: fetch ALL upcoming events, not just today/tomorrow
    today = datetime.utcnow().date()
    all_events = (
        fetch_mlb_fireworks_dates()
        + fetch_annual_fireworks_dates()
        + fetch_nps_fireworks_dates()
    )
    # Filter to next 90 days so test is meaningful
    upcoming = []
    seen = set()
    for e in all_events:
        try:
            d = datetime.strptime(e["date"], "%Y-%m-%d").date()
        except ValueError:
            continue
        if d < today:
            continue
        key = (e["date"], e["name"])
        if key in seen:
            continue
        seen.add(key)
        e["is_today"] = d == today
        upcoming.append(e)
    upcoming.sort(key=lambda x: x["date"])

    if not upcoming:
        await message.channel.send("No upcoming fireworks events found across any source.")
    else:
        summary = "\n".join(f"• **{e['date']}** — {e['name']} _(via {e['source']})_" for e in upcoming[:6])
        await message.channel.send(f"Found **{len(upcoming)}** upcoming fireworks event(s):\n{summary}\n\nPosting sample embed…")
        await message.channel.send(embed=embed_for_fireworks(upcoming[0]))


@bot.event
async def on_error(event, *args, **kwargs):
    log.exception("Unhandled error in event %s", event)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    init_db()
    log.info("Starting DC Safety Bot…")
    bot.run(DISCORD_TOKEN, log_handler=None)
