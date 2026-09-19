"""
Fetch NOAA Space Weather Prediction Center data and store it in Postgres (Neon).

Usage:
    python ingest_noaa.py           # fetch feeds and upsert new rows
    python ingest_noaa.py --init    # create tables/views first (safe to re-run)

Needs a DATABASE_URL environment variable (your Neon connection string).
Locally you can put it in a .env file; in GitHub Actions it comes from a secret.
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
import requests

try:  # optional: load .env when running locally
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

BASE = "https://services.swpc.noaa.gov"
FEEDS = {
    "kp": f"{BASE}/products/noaa-planetary-k-index.json",
    # Real-time solar wind. These replaced the old products/solar-wind/*-7-day.json
    # feeds in 2026 and hold about 1 day of data, from every spacecraft.
    "plasma": f"{BASE}/json/rtsw/rtsw_wind_1m.json",
    "mag": f"{BASE}/json/rtsw/rtsw_mag_1m.json",
    "xray": f"{BASE}/json/goes/primary/xrays-7-day.json",
}

# Re-upsert a few hours before the newest stored row, because NOAA
# sometimes revises recent values. Older rows are skipped to keep runs fast.
OVERLAP = timedelta(hours=3)

log = logging.getLogger("ingest_noaa")


# ---------------------------------------------------------------- fetching


def fetch_json(url: str, retries: int = 3):
    """GET a JSON feed, retrying with exponential backoff."""
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(
                url,
                timeout=30,
                headers={"User-Agent": "space-weather-dashboard (portfolio project)"},
            )
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as exc:
            if attempt == retries:
                raise
            wait = 2**attempt
            log.warning("Fetch failed (%s), retrying in %ss: %s", url, wait, exc)
            time.sleep(wait)


# ----------------------------------------------------------------- parsing


def to_records(data) -> list[dict]:
    """
    NOAA serves some feeds as a table ([header_row, row, row, ...]) and others
    as a list of objects ([{...}, {...}]). Return a list of dicts either way.
    """
    if not data:
        return []
    if isinstance(data[0], list):
        header = [str(h) for h in data[0]]
        return [dict(zip(header, row)) for row in data[1:]]
    return [rec for rec in data if isinstance(rec, dict)]


def pick(rec: dict, *names):
    """Return the first present key, tolerating naming changes (e.g. 'Kp' vs 'kp')."""
    for name in names:
        if name in rec:
            return rec[name]
    return None


def parse_time(value) -> datetime | None:
    """Parse NOAA timestamps like '2026-09-19 12:00:00.000' or '2026-09-19T12:00:00Z'."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def num(value) -> float | None:
    """Convert to float; NOAA uses strings, nulls and blanks for missing data."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def is_new(t: datetime | None, since: datetime | None) -> bool:
    return t is not None and (since is None or t >= since)


def kp_rows(data, since):
    rows = []
    for rec in to_records(data):
        t = parse_time(pick(rec, "time_tag", "time"))
        if not is_new(t, since):
            continue
        count = num(pick(rec, "station_count"))
        rows.append(
            (
                t,
                num(pick(rec, "Kp", "kp", "kp_index")),
                num(pick(rec, "a_running")),
                int(count) if count is not None else None,
            )
        )
    return rows


def is_active(rec: dict) -> bool:
    """RTSW feeds include every spacecraft; keep only the one SWPC marks as active."""
    return rec.get("active") in (True, "true", "True", 1, "1")


def solar_wind_rows(plasma, mag, since):
    """Merge the plasma and magnetic field feeds on timestamp (active spacecraft only)."""
    merged: dict[datetime, dict] = {}
    for rec in to_records(plasma):
        t = parse_time(rec.get("time_tag"))
        if is_new(t, since) and is_active(rec):
            merged.setdefault(t, {}).update(
                speed=num(pick(rec, "proton_speed", "speed")),
                density=num(pick(rec, "proton_density", "density")),
                temperature=num(pick(rec, "proton_temperature", "temperature")),
                plasma_source=rec.get("source"),
            )
    for rec in to_records(mag):
        t = parse_time(rec.get("time_tag"))
        if is_new(t, since) and is_active(rec):
            merged.setdefault(t, {}).update(
                bx_gsm=num(rec.get("bx_gsm")),
                by_gsm=num(rec.get("by_gsm")),
                bz_gsm=num(rec.get("bz_gsm")),
                bt=num(rec.get("bt")),
                mag_source=rec.get("source"),
            )
    cols = (
        "speed", "density", "temperature",
        "bx_gsm", "by_gsm", "bz_gsm", "bt",
        "plasma_source", "mag_source",
    )
    return [(t, *(r.get(c) for c in cols)) for t, r in sorted(merged.items())]


def xray_rows(data, since):
    rows = {}
    for rec in to_records(data):
        t = parse_time(rec.get("time_tag"))
        energy = rec.get("energy")
        if not is_new(t, since) or not energy:
            continue
        sat = num(rec.get("satellite"))
        # keyed so a duplicate (time, band) in the feed can't break the upsert
        rows[(t, energy)] = (t, energy, int(sat) if sat is not None else None, num(rec.get("flux")))
    return sorted(rows.values())


# ---------------------------------------------------------------- database

UPSERT_KP = """
INSERT INTO kp_index (time_tag, kp, a_running, station_count)
VALUES (%s, %s, %s, %s)
ON CONFLICT (time_tag) DO UPDATE SET
    kp = EXCLUDED.kp,
    a_running = EXCLUDED.a_running,
    station_count = EXCLUDED.station_count
"""

# COALESCE keeps an existing value if one feed is missing for that minute
UPSERT_SOLAR_WIND = """
INSERT INTO solar_wind (time_tag, speed, density, temperature,
                        bx_gsm, by_gsm, bz_gsm, bt, plasma_source, mag_source)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (time_tag) DO UPDATE SET
    speed       = COALESCE(EXCLUDED.speed,       solar_wind.speed),
    density     = COALESCE(EXCLUDED.density,     solar_wind.density),
    temperature = COALESCE(EXCLUDED.temperature, solar_wind.temperature),
    bx_gsm      = COALESCE(EXCLUDED.bx_gsm,      solar_wind.bx_gsm),
    by_gsm      = COALESCE(EXCLUDED.by_gsm,      solar_wind.by_gsm),
    bz_gsm      = COALESCE(EXCLUDED.bz_gsm,      solar_wind.bz_gsm),
    bt          = COALESCE(EXCLUDED.bt,          solar_wind.bt),
    plasma_source = COALESCE(EXCLUDED.plasma_source, solar_wind.plasma_source),
    mag_source    = COALESCE(EXCLUDED.mag_source,    solar_wind.mag_source)
"""

UPSERT_XRAY = """
INSERT INTO xray_flux (time_tag, energy, satellite, flux)
VALUES (%s, %s, %s, %s)
ON CONFLICT (time_tag, energy) DO UPDATE SET
    satellite = EXCLUDED.satellite,
    flux = EXCLUDED.flux
"""


def since_for(conn, table: str) -> datetime | None:
    """Newest stored timestamp minus the overlap window (None = backfill everything)."""
    latest = conn.execute(f"SELECT max(time_tag) FROM {table}").fetchone()[0]
    return latest - OVERLAP if latest else None


def upsert(conn, sql: str, rows: list) -> int:
    if rows:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
    conn.commit()
    return len(rows)


def init_schema(conn):
    schema = (Path(__file__).parent / "schema.sql").read_text()
    conn.execute(schema)
    conn.commit()
    log.info("Schema created/verified")


# -------------------------------------------------------------------- main


def run(conn) -> dict:
    """Ingest every dataset independently, so one failing feed doesn't block the rest."""
    jobs = {
        "kp_index": lambda: upsert(
            conn, UPSERT_KP, kp_rows(fetch_json(FEEDS["kp"]), since_for(conn, "kp_index"))
        ),
        "solar_wind": lambda: upsert(
            conn,
            UPSERT_SOLAR_WIND,
            solar_wind_rows(
                fetch_json(FEEDS["plasma"]),
                fetch_json(FEEDS["mag"]),
                since_for(conn, "solar_wind"),
            ),
        ),
        "xray_flux": lambda: upsert(
            conn, UPSERT_XRAY, xray_rows(fetch_json(FEEDS["xray"]), since_for(conn, "xray_flux"))
        ),
    }
    counts, errors = {}, {}
    for name, job in jobs.items():
        try:
            counts[name] = job()
            log.info("%-10s %d rows upserted", name, counts[name])
        except Exception as exc:  # noqa: BLE001 - record and carry on
            conn.rollback()
            errors[name] = f"{type(exc).__name__}: {exc}"
            log.error("%-10s FAILED: %s", name, errors[name])
    return {"counts": counts, "errors": errors}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--init", action="store_true", help="create tables and views first")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        log.error("DATABASE_URL is not set")
        return 2

    started = datetime.now(timezone.utc)
    with psycopg.connect(db_url, connect_timeout=30) as conn:
        if args.init:
            init_schema(conn)

        result = run(conn)
        status = "ok" if not result["errors"] else (
            "partial" if result["counts"] else "failed"
        )
        conn.execute(
            """INSERT INTO ingest_runs (started_at, finished_at, status, rows_upserted, errors)
               VALUES (%s, %s, %s, %s, %s)""",
            (
                started,
                datetime.now(timezone.utc),
                status,
                json.dumps(result["counts"]),
                json.dumps(result["errors"]) if result["errors"] else None,
            ),
        )
        conn.commit()

    log.info("Run finished: %s", status)
    # Non-zero exit makes GitHub Actions flag the run so failures get noticed
    return 0 if status == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
