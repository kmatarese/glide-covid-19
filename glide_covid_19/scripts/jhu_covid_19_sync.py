"""https://github.com/CSSEGISandData/COVID-19

Notes
-----
* All dates are in UTC (GMT+0)
* Stats are cumulative
"""

import os

from glide import *
from glide.extensions.pandas import *


OUTDIR = os.path.dirname(os.path.abspath(__file__)) + "/../data/"
OUTFILE = OUTDIR + "jhu_covid_timeseries.csv"

BASE_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/"
DATASETS = {
    "Confirmed": BASE_URL + "time_series_covid19_confirmed_global.csv",
    "Deaths": BASE_URL + "time_series_covid19_deaths_global.csv",
}
SOURCE_INDEX = ["Date", "Country/Region", "Province/State", "Lat", "Long"]
TARGET_INDEX = ["date", "country_region", "state_province", "lat", "long"]
VALUE_RENAMES = dict(Confirmed="cumulative_cases", Deaths="cumulative_deaths")


class CleanData(Node):
    def run(self, df, value_name):
        df["Date"] = pd.to_datetime(df["Date"])
        numerics = ["Lat", "Long", value_name]
        df[value_name] = df[value_name].replace("", 0)
        df[numerics] = df[numerics].apply(pd.to_numeric, downcast="integer")
        df.rename(
            columns={
                "Date": "date",
                "Country/Region": "country_region",
                "Province/State": "state_province",
                "Lat": "lat",
                "Long": "long",
                value_name: VALUE_RENAMES[value_name],
            },
            inplace=True,
        )
        df = df.set_index(TARGET_INDEX)
        self.push(df)


cleaner = Glider(
    DataFrameCSVExtract("extract")
    | DataFrameMethod("melt", method="melt", id_vars=SOURCE_INDEX[1:], var_name="Date")
    | CleanData("clean")
    | Return("return", flatten=True)
)

loader = Glider(
    Join("join", on=TARGET_INDEX) | LenPrint("print") | DataFrameCSVLoad("load")
)

cleaned = []
for name, url in DATASETS.items():
    rv = cleaner.consume(url, melt=dict(value_name=name), clean=dict(value_name=name))
    cleaned.append(rv)

loader.consume([cleaned], load=dict(f=OUTFILE))
