Glide COVID-19
==============

This is an example repo showing how one can use `glide` to sync/process data
related to the COVID-19 outbreak.

The `glide_covid_19/scripts` directory has examples that pull and mix data
from the following sources:

- [Johns Hopkins COVID-19 data](https://github.com/CSSEGISandData/COVID-19)
- [New York Times](https://github.com/nytimes/covid-19-data)
- [COVID Tracking Project](http://covidtracking.com/)
- [Starschema COVID-19 Data](https://github.com/starschema/COVID-19-data)
- US Census data (for FIPS/State tables)

The output is stored in the `glide_covid_19/data` directory. There are CSVs
as well as a sqlite database that contains (mostly) the same information. The
sqlite database has two timeseries tables:

- **country_province_timeseries**: a mix of JHU and CovidTracking.com data, as the
former lacks state-level data but the latter is US-specific.
- **us_county_timeseries**: NYT data with per-day counts added in.

It also has some side tables to join for more information regarding geos and
healthcare/hospital stats. See `glide_covid_19/data/sqlite_schema.sql` for a
full list of tables/columns.

Some import notes:

* Data is not currently being updated in an automated fashion.
* Some of my pandas-fu may be suboptimal but these are relatively small files
so things still run quickly.
* This has not been thoroughly tested, and is still prone to rapid changes.
* The datasources are somewhat disorganized when it comes to using
standardized values (such as UTC for timestamps or ISO-approved geos). This
may cause discrepancies, and there is room for improvement here.
* I may add more interesting side tables, such as demographic and population
data.

For more information about `glide`, go [here](https://github.com/kmatarese/glide).