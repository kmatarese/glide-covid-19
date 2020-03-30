# Poor man's pipeline...

fips_sync.py \
&& us_states_sync.py \
&& covidtracking_sync.py \
&& jhu_sync.py \
&& nytimes_sync.py \
&& starschema_sync.py \
&& country_province_timeseries.py
