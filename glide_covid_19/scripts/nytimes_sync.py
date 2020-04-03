#!/usr/bin/env python
"""https://github.com/nytimes/covid-19-data

Notes
-----
* Time zone of dates not currently specified
* Stats are cumulative
"""
from glide_covid_19.utils import *


OUTFILE = OUTDIR + "us_county_timeseries.csv"
URL = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"


class Transform(Node):
    def run(self, df):
        df.rename(
            columns=dict(cases="cumulative_cases", deaths="cumulative_deaths"),
            inplace=True,
        )
        df.drop(columns=["county", "state"], inplace=True)
        df.drop(df.loc[df["fips"].isnull()].index, inplace=True)

        # Calculate daily numbers from diff in cumulative
        df.set_index(["fips", "date"], inplace=True)
        df.sort_index(inplace=True)
        diff_columns = ["cumulative_cases", "cumulative_deaths"]
        df_diff = df.groupby("fips").apply(apply_df_diff, diff_columns)
        df["cases"] = df_diff["cumulative_cases"]
        df["deaths"] = df_diff["cumulative_deaths"]

        df.reset_index(inplace=True)
        df.set_index(["date", "fips"], inplace=True)
        df.sort_index(inplace=True)
        self.push(df)


if __name__ == "__main__":
    glider = Glider(node_template())
    glider["transform"] = Transform("transform")

    with get_sqlite_conn() as conn:
        conn.execute("DELETE FROM us_county_timeseries")
        glider.consume(
            URL,
            extract=dict(dtype=dict(fips=str)),
            csv_load=dict(f=OUTFILE),
            sql_load=dict(table="us_county_timeseries", conn=conn, if_exists="append"),
        )
        conn.commit()
