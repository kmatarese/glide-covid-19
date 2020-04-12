DROP TABLE IF EXISTS "fips";

CREATE TABLE "fips" (
  "fips" TEXT,
  "county_code" INTEGER,
  "name" TEXT,
  "name_long" TEXT,
  "population" INTEGER,
  "state_code_int" TEXT,
  "state_code_iso" TEXT,
  "state_code_postal" TEXT,
  "timezone" TEXT,
  "lat" NUMERIC,
  "long" NUMERIC,
  PRIMARY KEY(fips)
);


DROP TABLE IF EXISTS "iso1_geos";

CREATE TABLE "iso1_geos" (
  "iso1" TEXT,
  "admin_level" INTEGER,
  "name" TEXT,
  "osm_id" INTEGER,
  "population" INTEGER,
  "timezone" TEXT,
  "lat" NUMERIC,
  "long" NUMERIC,
  "wikidata_id" TEXT,
  "wikipedia_id" TEXT,
  PRIMARY KEY(iso1)
);


DROP TABLE IF EXISTS "iso2_geos";

CREATE TABLE "iso2_geos" (
  "iso2" TEXT,
  "iso1" TEXT,
  "admin_level" INTEGER,
  "name" TEXT,
  "osm_id" INTEGER,
  "population" INTEGER,
  "timezone" TEXT,
  "lat" NUMERIC,
  "long" NUMERIC,
  "wikidata_id" TEXT,
  "wikipedia_id" TEXT,
  PRIMARY KEY(iso2)
);


DROP TABLE IF EXISTS "iso2_healthcare_capacity";

CREATE TABLE "iso2_healthcare_capacity" (
  "iso2" TEXT,
  "hospital_beds" INTEGER,
  "total_chcs" INTEGER,
  "chc_service_delivery_sites" INTEGER,
  PRIMARY KEY(iso2)
);


DROP TABLE IF EXISTS "fips_icu_beds";

CREATE TABLE "fips_icu_beds" (
  "fips" TEXT,
  "hospitals" INTEGER,
  "icu_beds" INTEGER,
  PRIMARY KEY(fips)
);


DROP TABLE IF EXISTS "fips_timeseries";

CREATE TABLE "fips_timeseries" (
  "date" DATE,     
  "fips" TEXT,
  "cumulative_cases" INTEGER,
  "cumulative_deaths" INTEGER,
  "cases" INTEGER,
  "deaths" INTEGER,
  PRIMARY KEY(date, fips)
);


DROP TABLE IF EXISTS "iso2_timeseries";

CREATE TABLE "iso2_timeseries" (
  "date" DATE,
  "iso1" TEXT,
  "iso2" TEXT,
  "cumulative_cases" INTEGER,
  "cumulative_negative_tests" INTEGER,
  "cumulative_pending_tests" INTEGER,
  "cumulative_hospitalized" INTEGER,
  "cumulative_in_icu" INTEGER,
  "cumulative_on_ventilator" INTEGER,
  "cumulative_deaths" INTEGER,
  "cumulative_test_results" INTEGER,
  "cumulative_recovered" INTEGER,
  "currently_hospitalized" INTEGER,
  "currently_in_icu" INTEGER,
  "currently_on_ventilator" INTEGER,
  "cases" INTEGER,
  "hospitalized" INTEGER,
  "in_icu" INTEGER,
  "on_ventilator" INTEGER,
  "recovered" INTEGER,           
  "deaths" INTEGER, 
  "negative_tests" INTEGER,  
  "test_results" INTEGER,
  PRIMARY KEY(date, iso1, iso2)
);
