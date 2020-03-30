#!/usr/bin/env python
"""Combine country/province data from JHU and covidtracking.com into
a single timeseries.
"""
import pandas as pd

from glide_covid_19.utils import *


STATE_FILE = OUTDIR + "us_states.csv"
COVIDTRACKING_FILE = OUTDIR + "covidtracking_states_daily.csv"
JHU_FILE = OUTDIR + "jhu_covid_timeseries.csv"
OUTFILE = OUTDIR + "country_province_timeseries.csv"
EXTRACT_INDEX = ["country_region", "state_province", "date"]
TARGET_INDEX = ["date", "country_region", "state_province"]


class TransformCovidTracking(Node):
    def run(self, df, state_map):
        df = df.join(state_map, on=["state_abbr"], how="left")
        df["country_region"] = "United States of America"
        df.rename(columns=dict(state="state_province"), inplace=True)
        df.drop(columns=["state_abbr"], inplace=True)
        df.set_index(EXTRACT_INDEX, inplace=True)
        self.push(df)


class Combine(NoInputNode):
    def run(self, jhu_data, ct_data):
        us_idx = "United States of America"
        jhu_data.sort_index(inplace=True)
        # Delete data after this date, will replace with covidtracking data
        jhu_data.drop(jhu_data.loc[us_idx, :, "2020-03-04":].index, inplace=True)
        df = jhu_data.append(ct_data)
        df.reset_index(inplace=True)
        df.set_index(TARGET_INDEX, inplace=True)
        df.sort_index(inplace=True)
        self.push(df)


if __name__ == "__main__":
    state_map = pd.read_csv(
        STATE_FILE, index_col=["state_abbr"], usecols=["state_abbr", "state"]
    )

    jhu = Glider(DataFrameCSVExtract("extract") | Return("return", flatten=True))

    ct = Glider(
        DataFrameCSVExtract("extract")
        | TransformCovidTracking("transform", state_map=state_map)
        | Return("return", flatten=True)
    )

    loader = Glider(
        Combine("combine")
        | LenPrint("print")
        | [DataFrameCSVLoad("csv_load"), DataFrameSQLLoad("sql_load")]
    )

    jhu_data = jhu.consume(JHU_FILE, extract=dict(index_col=EXTRACT_INDEX))
    ct_data = ct.consume(COVIDTRACKING_FILE)

    with get_sqlite_conn() as conn:
        conn.execute("DELETE FROM country_province_timeseries")
        loader.consume(
            combine=dict(jhu_data=jhu_data, ct_data=ct_data),
            csv_load=dict(f=OUTFILE),
            sql_load=dict(
                table="country_province_timeseries", conn=conn, if_exists="append"
            ),
        )
        conn.commit()
