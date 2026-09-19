"""
Build the data file the dashboard page reads: site/data/dashboard.json

It combines recent history from Postgres (Kp, solar wind, X-rays) with the
latest NOAA OVATION aurora forecast, which is fetched fresh each run and kept
only in the JSON (the grid is ~65,000 points, too big to store hourly).
A small summary of each forecast is saved to the aurora_summary table.

Usage:
    python export_dashboard.py

Needs DATABASE_URL, like ingest_noaa.py.
"""

import base64
import json
import logging
import os
import shutil
import sys
import zlib
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from ingest_noaa import fetch_json, num, parse_time

OVATION_URL = "https://services.swpc.noaa.gov/json/ovation_aurora_latest.json"
OUT_FILE = Path(__file__).parent / "site" / "data" / "dashboard.json"
STORMS_DIR = OUT_FILE.parent / "storms"

# Save a full aurora snapshot when Kp reached this in the past 6 hours.
# Set STORM_KP=0 temporarily to test the storm gallery on a quiet day.
STORM_KP = float(os.environ.get("STORM_KP", 5))
GALLERY_SIZE = 8

# Probability (%) at which we count aurora as "likely visible" when working
# out how far towards the equator the oval reaches.
VISIBLE_THRESHOLD = 10

GRID_W, GRID_H = 360, 181  # 1 degree grid: lon -180..179, lat 90..-90

log = logging.getLogger("export_dashboard")


# ------------------------------------------------------------------ helpers


def iso(dt: datetime | None) -> str | None:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if dt else None


def rnd(value, digits=2):
    return round(value, digits) if value is not None else None


def flare_class(flux: float | None) -> str | None:
    """1.2e-6 -> 'C1.2' (NOAA's A/B/C/M/X scale from the 0.1-0.8 nm band)."""
    if flux is None or flux <= 0:
        return None
    for letter, base in (("X", 1e-4), ("M", 1e-5), ("C", 1e-6), ("B", 1e-7)):
        if flux >= base:
            return f"{letter}{flux / base:.1f}"
    return f"A{flux / 1e-8:.1f}"


def storm_level(kp: float | None) -> str | None:
    if kp is None:
        return None
    k = round(kp)
    return f"G{min(k - 4, 5)}" if k >= 5 else "None"


def stamp(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y%m%dT%H%MZ")


# ------------------------------------------------------------------- aurora


def aurora_grid(data: dict) -> bytearray:
    """
    Convert OVATION's [lon, lat, probability] list into a compact byte grid:
    row 0 = latitude 90, row 180 = latitude -90; column 0 = longitude -180.
    One byte per cell (0-100), so the whole globe is 65,160 bytes.
    """
    grid = bytearray(GRID_W * GRID_H)
    for point in data.get("coordinates") or []:
        if not isinstance(point, (list, tuple)) or len(point) < 3:
            continue
        lon, lat, value = num(point[0]), num(point[1]), num(point[2])
        if lon is None or lat is None or value is None:
            continue
        lat_i = int(round(lat))
        if not -90 <= lat_i <= 90:
            continue
        col = (int(round(lon)) + 180) % 360  # OVATION uses 0..359 east
        row = 90 - lat_i
        grid[row * GRID_W + col] = max(0, min(100, int(round(value))))
    return grid


def hemisphere_stats(grid: bytearray, rows: range) -> tuple[int, float | None]:
    """Peak probability and the latitude closest to the equator above the threshold."""
    peak, boundary = 0, None
    for row in rows:
        row_max = max(grid[row * GRID_W:(row + 1) * GRID_W])
        peak = max(peak, row_max)
        if row_max >= VISIBLE_THRESHOLD:
            lat = 90 - row
            if boundary is None or abs(lat) < abs(boundary):
                boundary = lat
    return peak, boundary


def build_aurora() -> tuple[dict | None, bytearray | None]:
    try:
        data = fetch_json(OVATION_URL)
    except Exception as exc:  # noqa: BLE001 - the page copes without it
        log.error("Aurora forecast unavailable: %s", exc)
        return None, None

    grid = aurora_grid(data)
    north_max, north_boundary = hemisphere_stats(grid, range(0, 90))
    south_max, south_boundary = hemisphere_stats(grid, range(91, 181))
    return {
        "observation_time": iso(parse_time(data.get("Observation Time"))),
        "forecast_time": iso(parse_time(data.get("Forecast Time"))),
        "north_max": north_max,
        "south_max": south_max,
        "north_boundary_lat": north_boundary,
        "south_boundary_lat": south_boundary,
        "width": GRID_W,
        "height": GRID_H,
        "grid_b64": base64.b64encode(bytes(grid)).decode("ascii"),
    }, grid


def save_aurora_summary(conn, aurora: dict):
    """Keep a small history of each forecast (not the full grid)."""
    if not aurora or not aurora["forecast_time"]:
        return
    try:
        conn.execute(
            """INSERT INTO aurora_summary (forecast_time, observation_time, north_max,
                   south_max, north_boundary_lat, south_boundary_lat)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (forecast_time) DO NOTHING""",
            (
                aurora["forecast_time"],
                aurora["observation_time"],
                aurora["north_max"],
                aurora["south_max"],
                aurora["north_boundary_lat"],
                aurora["south_boundary_lat"],
            ),
        )
        conn.commit()
    except psycopg.Error as exc:
        conn.rollback()
        log.warning("Couldn't save aurora summary (run ingest_noaa.py --init?): %s", exc)


# ------------------------------------------------------------ storm gallery


def save_storm_snapshot(conn, aurora: dict | None, grid: bytearray | None, payload: dict):
    """Keep the full grid, compressed, whenever there's been a storm recently."""
    if not aurora or grid is None or not aurora["forecast_time"]:
        return
    recent_kp = conn.execute("""
        SELECT max(kp) AS kp FROM kp_index
        WHERE time_tag >= now() - interval '6 hours'""").fetchone()["kp"]
    if recent_kp is None or recent_kp < STORM_KP:
        return
    wind = payload["solar_wind"]["latest"]
    try:
        conn.execute(
            """INSERT INTO aurora_snapshots (forecast_time, kp, solar_wind_speed, bz,
                   north_max, south_max, north_boundary_lat, south_boundary_lat, grid)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (forecast_time) DO NOTHING""",
            (
                aurora["forecast_time"], recent_kp, wind.get("speed"), wind.get("bz"),
                aurora["north_max"], aurora["south_max"],
                aurora["north_boundary_lat"], aurora["south_boundary_lat"],
                zlib.compress(bytes(grid), 9),
            ),
        )
        conn.commit()
        log.info("Storm snapshot saved (Kp %.1f)", recent_kp)
    except psycopg.Error as exc:
        conn.rollback()
        log.warning("Couldn't save storm snapshot (run ingest_noaa.py --init?): %s", exc)


def build_gallery(conn) -> list[dict]:
    """
    The strongest snapshot from each storm day, biggest storms first.
    Each grid goes in its own file so the page only downloads the ones people open.
    """
    try:
        rows = conn.execute(f"""
            SELECT * FROM (
                SELECT DISTINCT ON (date_trunc('day', forecast_time AT TIME ZONE 'UTC'))
                       forecast_time, kp, solar_wind_speed, bz, north_max, south_max,
                       north_boundary_lat, south_boundary_lat, grid
                FROM aurora_snapshots
                ORDER BY date_trunc('day', forecast_time AT TIME ZONE 'UTC'),
                         kp DESC, north_max DESC
            ) best
            ORDER BY kp DESC, forecast_time DESC
            LIMIT {GALLERY_SIZE}""").fetchall()
    except psycopg.Error as exc:
        conn.rollback()
        log.warning("Storm gallery unavailable (run ingest_noaa.py --init?): %s", exc)
        return []

    shutil.rmtree(STORMS_DIR, ignore_errors=True)
    STORMS_DIR.mkdir(parents=True, exist_ok=True)
    gallery = []
    for r in rows:
        name = f"{stamp(r['forecast_time'])}.json"
        meta = {
            "forecast_time": iso(r["forecast_time"]),
            "kp": rnd(r["kp"]),
            "storm": storm_level(r["kp"]),
            "speed": rnd(r["solar_wind_speed"], 0),
            "bz": rnd(r["bz"], 1),
            "north_max": r["north_max"],
            "south_max": r["south_max"],
            "north_boundary_lat": r["north_boundary_lat"],
            "south_boundary_lat": r["south_boundary_lat"],
        }
        grid_b64 = base64.b64encode(zlib.decompress(r["grid"])).decode("ascii")
        (STORMS_DIR / name).write_text(json.dumps({**meta, "grid_b64": grid_b64}, separators=(",", ":")))
        gallery.append({**meta, "file": f"data/storms/{name}"})
    return gallery


# ---------------------------------------------------------------- database


def query_all(conn, sql: str) -> list[dict]:
    return conn.execute(sql).fetchall()


def query_one(conn, sql: str) -> dict | None:
    return conn.execute(sql).fetchone()


def build_from_db(conn) -> dict:
    kp_rows = query_all(conn, """
        SELECT time_tag, kp FROM kp_index
        WHERE time_tag >= now() - interval '7 days' AND kp IS NOT NULL
        ORDER BY time_tag""")

    # 10-minute averages keep the file small and the charts smooth
    wind_rows = query_all(conn, """
        SELECT date_bin('10 minutes', time_tag, TIMESTAMPTZ '2000-01-01') AS t,
               avg(speed) AS speed, avg(density) AS density,
               avg(bz_gsm) AS bz, avg(bt) AS bt
        FROM solar_wind
        WHERE time_tag >= now() - interval '24 hours'
        GROUP BY 1 ORDER BY 1""")
    plasma = query_one(conn, """
        SELECT time_tag, speed, density FROM solar_wind
        WHERE speed IS NOT NULL ORDER BY time_tag DESC LIMIT 1""")
    mag = query_one(conn, """
        SELECT time_tag, bz_gsm, bt FROM solar_wind
        WHERE bz_gsm IS NOT NULL ORDER BY time_tag DESC LIMIT 1""")

    xray_rows = query_all(conn, """
        SELECT date_bin('10 minutes', time_tag, TIMESTAMPTZ '2000-01-01') AS t,
               max(flux) AS flux
        FROM xray_flux
        WHERE energy = '0.1-0.8nm' AND time_tag >= now() - interval '24 hours'
        GROUP BY 1 ORDER BY 1""")
    xray_latest = query_one(conn, """
        SELECT time_tag, flux FROM xray_flux
        WHERE energy = '0.1-0.8nm' AND flux IS NOT NULL
        ORDER BY time_tag DESC LIMIT 1""")
    xray_peak = query_one(conn, """
        SELECT time_tag, flux FROM xray_flux
        WHERE energy = '0.1-0.8nm' AND flux IS NOT NULL
          AND time_tag >= now() - interval '24 hours'
        ORDER BY flux DESC LIMIT 1""")

    last_run = query_one(conn, """
        SELECT finished_at, status FROM ingest_runs ORDER BY id DESC LIMIT 1""")

    latest_kp = kp_rows[-1] if kp_rows else None
    return {
        "last_ingest": {
            "finished_at": iso(last_run["finished_at"]) if last_run else None,
            "status": last_run["status"] if last_run else None,
        },
        "kp": {
            "latest": {
                "t": iso(latest_kp["time_tag"]),
                "kp": rnd(latest_kp["kp"]),
                "storm": storm_level(latest_kp["kp"]),
            } if latest_kp else None,
            "series": [[iso(r["time_tag"]), rnd(r["kp"])] for r in kp_rows],
        },
        "solar_wind": {
            "latest": {
                "t_plasma": iso(plasma["time_tag"]) if plasma else None,
                "speed": rnd(plasma["speed"], 0) if plasma else None,
                "density": rnd(plasma["density"], 1) if plasma else None,
                "t_mag": iso(mag["time_tag"]) if mag else None,
                "bz": rnd(mag["bz_gsm"], 1) if mag else None,
                "bt": rnd(mag["bt"], 1) if mag else None,
            },
            # [time, speed km/s, density p/cm3, Bz nT, Bt nT]
            "series": [
                [iso(r["t"]), rnd(r["speed"], 0), rnd(r["density"], 1),
                 rnd(r["bz"], 1), rnd(r["bt"], 1)]
                for r in wind_rows
            ],
        },
        "xray": {
            "latest": {
                "t": iso(xray_latest["time_tag"]),
                "flux": xray_latest["flux"],
                "class": flare_class(xray_latest["flux"]),
            } if xray_latest else None,
            "peak_24h": {
                "t": iso(xray_peak["time_tag"]),
                "flux": xray_peak["flux"],
                "class": flare_class(xray_peak["flux"]),
            } if xray_peak else None,
            "series": [[iso(r["t"]), r["flux"]] for r in xray_rows],
        },
    }


# -------------------------------------------------------------------- main


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        log.error("DATABASE_URL is not set")
        return 2

    aurora, grid = build_aurora()
    with psycopg.connect(db_url, connect_timeout=30, row_factory=dict_row) as conn:
        payload = build_from_db(conn)
        save_aurora_summary(conn, aurora)
        save_storm_snapshot(conn, aurora, grid, payload)
        storms = build_gallery(conn)

    payload = {
        "generated_at": iso(datetime.now(timezone.utc)),
        **payload,
        "aurora": aurora,
        "storms": storms,
    }

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text(json.dumps(payload, separators=(",", ":")))
    log.info("Wrote %s (%.0f KB, %d storms in gallery)",
             OUT_FILE, OUT_FILE.stat().st_size / 1024, len(storms))
    return 0


if __name__ == "__main__":
    sys.exit(main())
