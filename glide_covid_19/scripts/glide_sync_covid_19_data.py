from tlbx import st, pp
from glide import *
from glide.extensions.pandas import *

BASE_URL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/'
DATASETS = {
    "Confirmed": BASE_URL + 'time_series_19-covid-Confirmed.csv',
    "Deaths": BASE_URL + 'time_series_19-covid-Deaths.csv',
    "Recovered": BASE_URL + 'time_series_19-covid-Recovered.csv',
}
TARGET_INDEX = ["Country/Region", "Province/State", "Lat", "Long", "Date"]

class CleanData(Node):
    def run(self, df, value_name):
        df["Date"] = pd.to_datetime(df["Date"])
        numerics = ["Lat", "Long", value_name]
        df[value_name] = df[value_name].replace('', 0)
        df[numerics] = df[numerics].apply(pd.to_numeric, downcast='integer')
        df = df.set_index(TARGET_INDEX)
        self.push(df)

cleaner = Glider(
    CSVExtract("extract")
    | ToDataFrame("to_df")
    | DataFrameMethod(
        "melt", method="melt", id_vars=TARGET_INDEX[:-1], var_name="Date"
    )
    | CleanData("clean")
    | Return("return", flatten=True)
)

loader = Glider(
    Join("join", on=TARGET_INDEX)
    | DataFrameCSVLoad("load")
)

cleaned = []
for name, url in DATASETS.items():
    rv = cleaner.consume(
        url, melt=dict(value_name=name), clean=dict(value_name=name)
    )
    cleaned.append(rv)

loader.consume([cleaned], load=dict(f="/tmp/covid_timeseries.csv"))
