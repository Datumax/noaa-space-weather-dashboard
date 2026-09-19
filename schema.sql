-- NOAA space weather schema for Postgres (Neon)
-- Safe to run more than once: everything uses IF NOT EXISTS / OR REPLACE.
-- Each table's primary key on time_tag doubles as the index that makes
-- "last 24 hours" / "last 7 days" dashboard queries fast.

-- Planetary K-index: geomagnetic activity, one value every 3 hours (0 to 9)
CREATE TABLE IF NOT EXISTS kp_index (
    time_tag      TIMESTAMPTZ PRIMARY KEY,
    kp            REAL,
    a_running     REAL,
    station_count SMALLINT
);

-- Solar wind from whichever L1 spacecraft SWPC marks active (DSCOVR, ACE,
-- and newer missions), one row per minute.
-- Plasma (speed, density, temperature) and magnetic field come from two
-- separate NOAA feeds and are merged on timestamp.
CREATE TABLE IF NOT EXISTS solar_wind (
    time_tag    TIMESTAMPTZ PRIMARY KEY,
    speed       REAL,   -- km/s
    density     REAL,   -- protons per cm^3
    temperature REAL,   -- kelvin
    bx_gsm      REAL,   -- nT
    by_gsm      REAL,   -- nT
    bz_gsm      REAL,   -- nT; strongly negative = aurora more likely
    bt          REAL,   -- nT; total field strength
    plasma_source TEXT, -- spacecraft that supplied the plasma reading
    mag_source    TEXT  -- spacecraft that supplied the magnetic field reading
);

-- Added after NOAA's 2026 feed change; safe on tables created earlier
ALTER TABLE solar_wind ADD COLUMN IF NOT EXISTS plasma_source TEXT;
ALTER TABLE solar_wind ADD COLUMN IF NOT EXISTS mag_source    TEXT;

-- GOES X-ray flux, one row per minute per wavelength band.
--   '0.1-0.8nm'  = long band, used to classify flares (A/B/C/M/X)
--   '0.05-0.4nm' = short band
CREATE TABLE IF NOT EXISTS xray_flux (
    time_tag  TIMESTAMPTZ NOT NULL,
    energy    TEXT        NOT NULL,
    satellite SMALLINT,
    flux      DOUBLE PRECISION,  -- W/m^2
    PRIMARY KEY (time_tag, energy)
);

-- One row per ingest run: powers a "last updated" label on the dashboard
-- and makes failures easy to spot.
CREATE TABLE IF NOT EXISTS ingest_runs (
    id          BIGSERIAL PRIMARY KEY,
    started_at  TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    status      TEXT        NOT NULL,  -- 'ok', 'partial' or 'failed'
    rows_upserted JSONB,
    errors      JSONB
);

-- Flare class for every long-band reading
CREATE OR REPLACE VIEW xray_flare_class AS
SELECT
    time_tag,
    flux,
    CASE
        WHEN flux IS NULL  THEN NULL
        WHEN flux >= 1e-4  THEN 'X'
        WHEN flux >= 1e-5  THEN 'M'
        WHEN flux >= 1e-6  THEN 'C'
        WHEN flux >= 1e-7  THEN 'B'
        ELSE 'A'
    END AS flare_class
FROM xray_flux
WHERE energy = '0.1-0.8nm';

-- NOAA geomagnetic storm scale (G1 to G5) from Kp.
-- Kp is reported in thirds (e.g. 4.67 = "5-"), so it's rounded first.
CREATE OR REPLACE VIEW kp_storm_level AS
SELECT
    time_tag,
    kp,
    CASE
        WHEN kp IS NULL            THEN NULL
        WHEN ROUND(kp::numeric) >= 9 THEN 'G5'
        WHEN ROUND(kp::numeric) >= 8 THEN 'G4'
        WHEN ROUND(kp::numeric) >= 7 THEN 'G3'
        WHEN ROUND(kp::numeric) >= 6 THEN 'G2'
        WHEN ROUND(kp::numeric) >= 5 THEN 'G1'
        ELSE 'None'
    END AS storm_level
FROM kp_index;

-- One row per OVATION aurora forecast (a summary only; the full grid is
-- ~65,000 points and lives in the dashboard's JSON file instead)
CREATE TABLE IF NOT EXISTS aurora_summary (
    forecast_time      TIMESTAMPTZ PRIMARY KEY,
    observation_time   TIMESTAMPTZ,
    north_max          SMALLINT,  -- peak probability %, northern hemisphere
    south_max          SMALLINT,
    north_boundary_lat REAL,      -- lowest latitude with >= 10% probability
    south_boundary_lat REAL
);

-- Full aurora grids saved only during geomagnetic storms (Kp >= 5), for the
-- storm gallery. Grids are zlib-compressed: mostly zeros, so ~65 KB shrinks
-- to a few KB each.
CREATE TABLE IF NOT EXISTS aurora_snapshots (
    forecast_time      TIMESTAMPTZ PRIMARY KEY,
    kp                 REAL,
    solar_wind_speed   REAL,
    bz                 REAL,
    north_max          SMALLINT,
    south_max          SMALLINT,
    north_boundary_lat REAL,
    south_boundary_lat REAL,
    grid               BYTEA NOT NULL
);
