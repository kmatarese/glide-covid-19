# Poor man's pipeline...

iso_geos_sync.py \
&& covidtracking_sync.py \
&& jhu_sync.py \
&& nytimes_sync.py \
&& starschema_sync.py \
&& iso2_timeseries.py
