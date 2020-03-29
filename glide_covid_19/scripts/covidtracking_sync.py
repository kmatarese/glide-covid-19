"""http://covidtracking.com/api/states/daily.csv

Notes
-----
* Time zone is  EST
"""

import os

from glide import *
from glide.extensions.pandas import *


OUTDIR = os.path.dirname(os.path.abspath(__file__)) + "/../data/"
OUTFILE = OUTDIR + "covidtracking_states_daily.csv"
URL = "http://covidtracking.com/api/states/daily.csv"


class CleanData(Node):
    def run(self, df):
        df.drop(columns=["total", "dateChecked", "hash"], inplace=True)
        df.rename(
            columns={
                "state": "state_abbr",
                "positive": "cumulative_cases",
                "positiveIncrease": "cases",
                "negative": "cumulative_negative_tests",
                "negativeIncrease": "negative_tests",
                "pending": "cumulative_pending_tests",
                "totalTestResults": "cumulative_test_results",
                "totalTestResultsIncrease": "test_results",
                "hospitalized": "cumulative_hospitalized",
                "hospitalizedIncrease": "hospitalized",
                "death": "cumulative_deaths",
                "deathIncrease": "deaths",
            },
            inplace=True,
        )
        df["fips"] = df["fips"].astype(str).str.zfill(5)
        df["date"] = pd.to_datetime(df["date"].astype(str))
        df.set_index(["date", "fips"], inplace=True)
        self.push(df)


glider = Glider(
    DataFrameCSVExtract("extract") | CleanData("clean") | DataFrameCSVLoad("load")
)

glider.consume(URL, load=dict(f=OUTFILE))
