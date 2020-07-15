DROP VIEW IF EXISTS "fips_stats_view";

CREATE VIEW fips_stats_view AS
SELECT
 f.fips,
 f.population
FROM
 fips f;


DROP VIEW IF EXISTS "iso1_stats_view";

CREATE VIEW iso1_stats_view AS
SELECT
 g.iso1,
 g.population
FROM
 iso1_geos g;


DROP VIEW IF EXISTS "iso2_stats_view";

CREATE VIEW iso2_stats_view AS
SELECT
 g.iso2,
 g.population
FROM
 iso2_geos g;


DROP VIEW IF EXISTS "fips_timeseries_view";

CREATE VIEW fips_timeseries_view AS
SELECT
 t.*,
 i.hospitals,
 i.icu_beds
FROM
 fips_timeseries t
 LEFT JOIN fips_icu_beds i ON t.fips = i.fips;


DROP VIEW IF EXISTS "iso2_timeseries_view";

CREATE VIEW iso2_timeseries_view AS
SELECT
 c.*,
 hc.hospital_beds,
 hc.total_chcs,
 hc.chc_service_delivery_sites,
 g.name as iso2_name,
 g.timezone as iso2_timezone,
 g.lat as iso2_lat,
 g.long as iso2_long
FROM
 iso2_timeseries c
 LEFT JOIN iso2_healthcare_capacity hc ON c.iso2=hc.iso2
 LEFT JOIN iso2_geos g on c.iso2=g.iso2

