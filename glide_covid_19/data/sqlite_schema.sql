
DROP TABLE IF EXISTS "healthcare_capacity";

CREATE TABLE "healthcare_capacity" (
  "country_region" TEXT,
  "state_province" TEXT,
  "hospital_beds" INTEGER,
  "total_chcs" INTEGER,
  "chc_service_delivery_sites" INTEGER,
  PRIMARY KEY(country_region, state_province)
);

DROP TABLE IF EXISTS "icu_beds_by_fips";

CREATE TABLE "icu_beds_by_fips" (
  "fips" TEXT,
  "hospitals" INTEGER,
  "icu_beds" INTEGER,
  PRIMARY KEY(fips)
);

DROP TABLE IF EXISTS "fips";

CREATE TABLE "fips" (
  "fips" TEXT,
  "county_name" TEXT,
  "state_abbr" TEXT,                
  "lat" NUMERIC,
  "long" NUMERIC,
  PRIMARY KEY(fips)
);

DROP TABLE IF EXISTS "us_states";

CREATE TABLE "us_states" (
  "state_abbr" TEXT,
  "state" TEXT,
  "fips_code" TEXT,
  "population" INTEGER,
  "population_density" NUMERIC,
  PRIMARY KEY(state_abbr)
);

DROP TABLE IF EXISTS "us_county_timeseries";

CREATE TABLE "us_county_timeseries" (
  "date" DATE,     
  "fips" TEXT,
  "cumulative_cases" INTEGER,
  "cumulative_deaths" INTEGER,
  "cases" INTEGER,
  "deaths" INTEGER,
  PRIMARY KEY(date, fips)
);

DROP TABLE IF EXISTS "country_province_timeseries";

CREATE TABLE "country_province_timeseries" (
  "date" DATE,     
  "country_region" TEXT,
  "state_province" TEXT,
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
  "deaths" INTEGER, 
  "negative_tests" INTEGER,  
  "test_results" INTEGER,
  PRIMARY KEY(date, country_region, state_province)
);


