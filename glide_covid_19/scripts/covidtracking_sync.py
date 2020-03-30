#!/usr/bin/env python
"""http://covidtracking.com/api/states/daily.csv

Notes
-----
* Time zone is  EST
"""
from glide_covid_19.utils import *


OUTFILE = OUTDIR + "covidtracking_states_daily.csv"
URL = "http://covidtracking.com/api/states/daily.csv"


class Transform(Node):
    def run(self, df):
        df.drop(columns=["total", "dateChecked", "hash", "fips"], inplace=True)
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
        df["date"] = pd.to_datetime(df["date"].astype(str))
        df.set_index(["date", "state_abbr"], inplace=True)
        df.sort_index(inplace=True)
        self.push(df)


if __name__ == "__main__":
    glider = Glider(
        DataFrameCSVExtract("extract")
        | Transform("transform")
        | LenPrint("print")
        | DataFrameCSVLoad("load")
    )
    glider.consume(URL, load=dict(f=OUTFILE))
