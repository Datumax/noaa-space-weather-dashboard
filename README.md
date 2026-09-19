# Space Weather Dashboard

A mobile-friendly dashboard with a spinnable 3D globe showing the live aurora
forecast, plus geomagnetic activity, solar wind and solar flares.

Every hour, GitHub Actions collects NOAA Space Weather Prediction Center data
into Postgres (Neon), builds a small JSON snapshot, and publishes the static
page to GitHub Pages. The browser never touches the database.

| Table | Source | Resolution |
|---|---|---|
| `kp_index` | Planetary K-index | 3-hourly |
| `solar_wind` | Active L1 spacecraft, plasma + magnetic field (merged) | 1 minute |
| `xray_flux` | GOES X-ray flux, both bands | 1 minute |
| `ingest_runs` | Log of every run | per run |

| `aurora_summary` | OVATION aurora forecast (summary only) | hourly |
| `aurora_snapshots` | Full aurora grid, compressed, during storms (Kp 5+) | hourly while stormy |

Views: `xray_flare_class` (A/B/C/M/X) and `kp_storm_level` (G1 to G5).

The full aurora grid (~65,000 points) is kept only in the JSON snapshot, packed
as one byte per cell, because storing it hourly would fill the free database
within weeks.

## Storm gallery

When Kp has reached 5 in the past 6 hours, `export_dashboard.py` saves the full
aurora grid (zlib-compressed to a few KB) to `aurora_snapshots`. The page shows
the strongest snapshot from each storm day, and each grid is a separate file
that's only downloaded when someone opens it.

To test on a quiet day (PowerShell):
```
$env:STORM_KP=0; python export_dashboard.py; Remove-Item Env:STORM_KP
```
Then remove the test snapshot in Neon: `DELETE FROM aurora_snapshots WHERE kp < 5;`

## Preview the dashboard locally

```
python export_dashboard.py
python -m http.server 8000 -d site
```
Then open http://localhost:8000.

## Setup

1. `pip install -r requirements.txt`
2. Copy `.env.example` to `.env` and paste your Neon connection string.
3. First run creates the tables and backfills what NOAA currently holds
   (about 7 days for Kp and X-rays, about 1 day for solar wind):
   `python ingest_noaa.py --init`
4. Push to GitHub, add the connection string as a repository secret named
   `DATABASE_URL` (Settings, then Secrets and variables, then Actions),
   and the workflow runs hourly.

## Design notes

- NOAA feeds hold a rolling window (about 1 day for solar wind, 7 days for
  the rest), so hourly polling loses nothing
  and keeps Neon's compute-hours low.
- Each run only upserts rows newer than the latest stored one (minus a
  3-hour overlap, as NOAA sometimes revises recent values).
- Each feed is ingested independently, so one outage doesn't block the others.
- Storage estimate: roughly 200 MB a year, within Neon's 0.5 GB free tier
  for about two years before a retention policy is needed.
