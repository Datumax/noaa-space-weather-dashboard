# Space Weather Dashboard: data ingest

Collects NOAA Space Weather Prediction Center data every hour and stores it
in Postgres (Neon), building up history the NOAA feeds themselves don't keep.

| Table | Source | Resolution |
|---|---|---|
| `kp_index` | Planetary K-index | 3-hourly |
| `solar_wind` | DSCOVR plasma + magnetic field (merged) | 1 minute |
| `xray_flux` | GOES X-ray flux, both bands | 1 minute |
| `ingest_runs` | Log of every run | per run |

Views: `xray_flare_class` (A/B/C/M/X) and `kp_storm_level` (G1 to G5).

## Setup

1. `pip install -r requirements.txt`
2. Copy `.env.example` to `.env` and paste your Neon connection string.
3. First run creates the tables and backfills about 7 days:
   `python ingest_noaa.py --init`
4. Push to GitHub, add the connection string as a repository secret named
   `DATABASE_URL` (Settings, then Secrets and variables, then Actions),
   and the workflow runs hourly.

## Design notes

- NOAA feeds hold a rolling ~7-day window, so hourly polling loses nothing
  and keeps Neon's compute-hours low.
- Each run only upserts rows newer than the latest stored one (minus a
  3-hour overlap, as NOAA sometimes revises recent values).
- Each feed is ingested independently, so one outage doesn't block the others.
- Storage estimate: roughly 200 MB a year, within Neon's 0.5 GB free tier
  for about two years before a retention policy is needed.
