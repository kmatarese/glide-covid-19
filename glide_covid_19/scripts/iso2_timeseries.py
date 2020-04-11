#!/usr/bin/env python
"""Combine country/province data from JHU and covidtracking.com into
a single timeseries.
"""
import pandas as pd

from glide_covid_19.utils import *


COVIDTRACKING_FILE = OUTDIR + "covidtracking_states_daily.csv"
JHU_FILE = OUTDIR + "jhu_covid_timeseries.csv"
OUTFILE = OUTDIR + "iso2_timeseries.csv"
EXTRACT_INDEX = ["iso1", "iso2", "date"]
TARGET_INDEX = ["date", "iso1", "iso2"]


class TransformCovidTracking(Node):
    def run(self, df):
        df["iso1"] = "US"
        df.set_index(EXTRACT_INDEX, inplace=True)
        self.push(df)


class Combine(NoInputNode):
    def run(self, jhu_data, ct_data):
        us_idx = "US"
        # NOTE: pandas was hitting an error if not explicitly setting level param
        jhu_data.sort_index(inplace=True, level=[0, 1, 2])
        # Delete data after this date, will replace with covidtracking data
        jhu_data.drop(jhu_data.loc[us_idx, :, "2020-03-04":].index, inplace=True)
        df = jhu_data.append(ct_data)
        df.reset_index(inplace=True)
        df.set_index(TARGET_INDEX, inplace=True)
        df.sort_index(inplace=True)
        self.push(df)


if __name__ == "__main__":
    jhu = Glider(DataFrameCSVExtract("extract") | Return("return", flatten=True))

    ct = Glider(
        DataFrameCSVExtract("extract")
        | TransformCovidTracking("transform")
        | Return("return", flatten=True)
    )

    loader = Glider(
        Combine("combine")
        | LenPrint("print")
        | [DataFrameCSVLoad("csv_load"), DataFrameSQLLoad("sql_load")]
    )

    print("Combining JHU and covidtracking data")

    jhu_data = jhu.consume(JHU_FILE, extract=dict(index_col=EXTRACT_INDEX))
    ct_data = ct.consume(COVIDTRACKING_FILE)

    with get_sqlite_conn() as conn:
        conn.execute("DELETE FROM iso2_timeseries")
        loader.consume(
            combine=dict(jhu_data=jhu_data, ct_data=ct_data),
            csv_load=dict(f=OUTFILE),
            sql_load=dict(table="iso2_timeseries", conn=conn, if_exists="append"),
        )
        conn.commit()
