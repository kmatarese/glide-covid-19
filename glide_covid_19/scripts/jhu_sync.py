#!/usr/bin/env python
"""https://github.com/CSSEGISandData/COVID-19

Notes
-----
* All dates are in UTC (GMT+0)
* Stats are cumulative
"""
from glide_covid_19.utils import *


OUTFILE = OUTDIR + "jhu_covid_timeseries.csv"

BASE_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/"
DATASETS = {
    "Confirmed": BASE_URL + "time_series_covid19_confirmed_global.csv",
    "Deaths": BASE_URL + "time_series_covid19_deaths_global.csv",
}
SOURCE_INDEX = ["Date", "Country/Region", "Province/State", "Lat", "Long"]
GROUP_INDEX = ["country_region", "state_province", "date"]
TARGET_INDEX = ["date", "country_region", "state_province"]
VALUE_RENAMES = dict(Confirmed="cumulative_cases", Deaths="cumulative_deaths")


class Transform(Node):
    def run(self, df, value_name):
        df.drop(columns=["Lat", "Long"], inplace=True)

        df["Date"] = pd.to_datetime(df["Date"])
        df["Date"] = df["Date"].dt.date

        # TODO: Confirm all country and province names conform to ISO-3166
        df["Province/State"].fillna("Unknown", inplace=True)
        # https://en.wikipedia.org/wiki/ISO_3166-1
        df.loc[
            df["Country/Region"] == "US", "Country/Region"
        ] = "United States of America"

        df[value_name] = df[value_name].replace("", 0)
        df[value_name] = df[value_name].apply(pd.to_numeric, downcast="integer")

        value_rename = VALUE_RENAMES[value_name]
        df.rename(
            columns={
                "Date": "date",
                "Country/Region": "country_region",
                "Province/State": "state_province",
                value_name: value_rename,
            },
            inplace=True,
        )

        df.set_index(GROUP_INDEX, inplace=True)
        df.sort_index(inplace=True)

        def diff(x):
            first_index = x.first_valid_index()
            first_values = x.loc[first_index]
            x = x.diff()
            x.loc[first_index, value_rename] = first_values[value_rename]
            return x

        df_diff = df.groupby(["country_region", "state_province"]).apply(diff)
        non_cumulative_name = value_rename.replace("cumulative_", "")
        df[non_cumulative_name] = df_diff[value_rename]

        df.reset_index(inplace=True)
        df.set_index(TARGET_INDEX, inplace=True)
        df.sort_index(inplace=True)
        self.push(df)


if __name__ == "__main__":
    cleaner = Glider(
        DataFrameCSVExtract("extract")
        | DataFrameMethod(
            "melt", method="melt", id_vars=SOURCE_INDEX[1:], var_name="Date"
        )
        | Transform("transform")
        | Return("return", flatten=True)
    )

    loader = Glider(
        Join("join", on=TARGET_INDEX) | LenPrint("print") | DataFrameCSVLoad("load")
    )

    cleaned = []
    for name, url in DATASETS.items():
        rv = cleaner.consume(
            url, melt=dict(value_name=name), transform=dict(value_name=name)
        )
        cleaned.append(rv)

    loader.consume([cleaned], load=dict(f=OUTFILE))
