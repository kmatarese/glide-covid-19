#!/usr/bin/env python

import numpy as np

from glide_covid_19.utils import *


OUTFILE = OUTDIR + "us_states.csv"
URL = "https://raw.githubusercontent.com/COVID19Tracking/associated-data/master/us_census_data/us_census_2018_population_estimates_states.csv"
APPEND_STATES = [
    dict(
        state_abbr="AS",
        state="American Samoa",
        fips_code="60",
        population=55641,
        population_density=np.nan,
    ),
    dict(
        state_abbr="GU",
        state="Guam",
        fips_code="66",
        population=164229,
        population_density=np.nan,
    ),
    dict(
        state_abbr="MP",
        state="Northern Mariana Islands",
        fips_code="78",
        population=54144,
        population_density=np.nan,
    ),
    dict(
        state_abbr="VI",
        state="U.S. Virgin Islands",
        fips_code="78",
        population=107268,
        population_density=np.nan,
    ),
]


class Transform(Node):
    def run(self, df):
        df.rename(
            columns=dict(
                state="state_abbr",
                state_name="state",
                geo_id="fips_code",
                pop_density="population_density",
            ),
            inplace=True,
        )

        df = df.append(APPEND_STATES, ignore_index=True)
        self.push(df)


if __name__ == "__main__":
    glider = Glider(node_template())
    glider["transform"] = Transform("transform")

    with get_sqlite_conn() as conn:
        conn.execute("DELETE FROM us_states")
        glider.consume(
            URL,
            extract=dict(dtype=dict(geo_id=str)),
            csv_load=dict(f=OUTFILE, index=False),
            sql_load=dict(
                table="us_states", conn=conn, if_exists="append", index=False
            ),
        )
        conn.commit()
