Glide COVID-19
==============

This is an example repo showing how one can use
[glide](https://github.com/kmatarese/glide) to sync/process data related to
the COVID-19 outbreak. The results are cleaned and compiled into a sqlite
database that can be used as a starting point for analysis.

The `glide_covid_19/scripts` directory has scripts that pull and mix data
from the following sources:

- [Johns Hopkins COVID-19 data](https://github.com/CSSEGISandData/COVID-19)
- [New York Times](https://github.com/nytimes/covid-19-data)
- [COVID Tracking Project](http://covidtracking.com/)
- [Starschema COVID-19 Data](https://github.com/starschema/COVID-19-data)
- [ISO Geo Data](https://github.com/hyperknot/country-levels)

See `glide_covid_19/data/sqlite_schema.sql` for a full list of tables/columns.

Some important notes:

* Data is not currently being updated in an automated fashion.
* This is meant to be a quick set of examples. There is room for improvement.
* This is still prone to rapid changes.
* This is not meant to verify or validate the data sources themselves, which
may contain errors and discrepancies.
