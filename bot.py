"""
DC Safety Bot
Monitors crime incidents near 1345 S Capitol St SW, Washington DC
and posts alerts to a Discord channel every 15 minutes.
Also monitors for Nationals Park fireworks nights.
"""

import os
import sqlite3
import logging
import asyncio
import math
import requests
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup

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

# Nationals schedule page
NATS_SCHEDULE_URL = "https://www.mlb.com/nationals/schedule"

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
def fetch_fireworks_dates() -> list[str]:
    """
    Scrape the Nationals schedule page for games tagged with 'fireworks'.
    Returns list of date strings like '2026-04-05'.
    """
    try:
        resp = requests.get(NATS_SCHEDULE_URL, timeout=20, headers={
            "User-Agent": "Mozilla/5.0 DC-Safety-Bot/1.0"
        })
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception as exc:
        log.warning("Fireworks schedule fetch error: %s", exc)
        return []

    fireworks_dates = []
    # Look for any element whose text mentions "fireworks"
    for tag in soup.find_all(string=lambda t: t and "firework" in t.lower()):
        # Walk up to find a date attribute
        parent = tag.parent
        for _ in range(8):
            if parent is None:
                break
            date_val = parent.get("data-date") or parent.get("datetime") or ""
            if date_val and len(date_val) >= 10:
                try:
                    d = datetime.strptime(date_val[:10], "%Y-%m-%d").date()
                    fireworks_dates.append(str(d))
                except ValueError:
                    pass
                break
            parent = parent.parent

    # Deduplicate
    fireworks_dates = list(set(fireworks_dates))
    log.info("Fireworks dates found: %s", fireworks_dates)
    return fireworks_dates


def embed_for_fireworks(event_date: str, is_today: bool) -> discord.Embed:
    when = "**TONIGHT**" if is_today else f"**TOMORROW** ({event_date})"
    embed = discord.Embed(
        title="🎆 Nationals Park Fireworks Night!",
        description=(
            f"Fireworks are scheduled {when} at Nationals Park.\n\n"
            "Expect **heavy traffic, road closures, and noise** near 1345 S Capitol St SW. "
            "Plan accordingly!"
        ),
        colour=discord.Colour.blue(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_footer(text="Source: mlb.com/nationals/schedule")
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

    today = datetime.utcnow().date()
    tomorrow = today + timedelta(days=1)

    dates = fetch_fireworks_dates()
    for event_date in dates:
        try:
            d = datetime.strptime(event_date, "%Y-%m-%d").date()
        except ValueError:
            continue

        is_today = d == today
        is_tomorrow = d == tomorrow

        if not (is_today or is_tomorrow):
            continue

        alert_key = f"{event_date}_{'today' if is_today else 'tomorrow'}"
        if fireworks_already_alerted(alert_key):
            continue

        embed = embed_for_fireworks(event_date, is_today)
        try:
            await channel.send(embed=embed)
            mark_fireworks_alerted(alert_key)
            log.info("Posted fireworks alert for %s", event_date)
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
    await message.channel.send("🔍 Scraping mlb.com for fireworks dates…")

    dates = fetch_fireworks_dates()
    if not dates:
        # Post a sample embed so the format can be verified
        await message.channel.send(
            content="_(No fireworks dates found on mlb.com right now — showing sample embed.)_",
            embed=embed_for_fireworks("2026-07-04", False),
        )
    else:
        await message.channel.send(
            content=f"Found **{len(dates)}** fireworks date(s): {', '.join(sorted(dates))}. "
                    "Posting alert embed(s)…"
        )
        for event_date in sorted(dates)[:3]:   # cap at 3 to avoid spam
            try:
                d = datetime.strptime(event_date, "%Y-%m-%d").date()
            except ValueError:
                continue
            is_today = d == datetime.utcnow().date()
            await message.channel.send(embed=embed_for_fireworks(event_date, is_today))


@bot.event
async def on_error(event, *args, **kwargs):
    log.exception("Unhandled error in event %s", event)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    init_db()
    log.info("Starting DC Safety Bot…")
    bot.run(DISCORD_TOKEN, log_handler=None)
