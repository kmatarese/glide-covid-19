"""This was inspired by the following git repo/script which uses a
library called `dataflows`:

https://github.com/datasets/covid-19/blob/master/scripts/process.py

Their example does a little more, and produces another file or two, but I
wanted to show how something similar could be done with glide.
"""

from glide import *
from glide.extensions.pandas import *

BASE_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/"
DATASETS = {
    "Confirmed": BASE_URL + "time_series_covid19_confirmed_global.csv",
    "Deaths": BASE_URL + "time_series_covid19_deaths_global.csv"
}
TARGET_INDEX = ["Country/Region", "Province/State", "Lat", "Long", "Date"]


class CleanData(Node):
    def run(self, df, value_name):
        df["Date"] = pd.to_datetime(df["Date"])
        numerics = ["Lat", "Long", value_name]
        df[value_name] = df[value_name].replace("", 0)
        df[numerics] = df[numerics].apply(pd.to_numeric, downcast="integer")
        df = df.set_index(TARGET_INDEX)
        self.push(df)


cleaner = Glider(
    CSVExtract("extract")
    | ToDataFrame("to_df")
    | DataFrameMethod("melt", method="melt", id_vars=TARGET_INDEX[:-1], var_name="Date")
    | CleanData("clean")
    | Return("return", flatten=True)
)

loader = Glider(
    Join("join", on=TARGET_INDEX)
    | LenPrint("print")
    | DataFrameCSVLoad("load")
)

cleaned = []
for name, url in DATASETS.items():
    rv = cleaner.consume(url, melt=dict(value_name=name), clean=dict(value_name=name))
    cleaned.append(rv)

loader.consume([cleaned], load=dict(f="/tmp/covid_timeseries.csv"))
