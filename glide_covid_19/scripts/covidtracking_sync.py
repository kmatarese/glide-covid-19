#!/usr/bin/env python
"""http://covidtracking.com/api/states/daily.csv"""

from glide_covid_19.utils import *


OUTFILE = OUTDIR + "covidtracking_states_daily.csv"
URL = "http://covidtracking.com/api/v1/states/daily.csv"


KEEP_COLUMNS = [
    "iso2",
    "date",
    "cumulative_cases",
    "cumulative_negative_tests",
    "cumulative_pending_tests",
    "currently_hospitalized",
    "currently_in_icu",
    "cumulative_in_icu",
    "currently_on_ventilator",
    "cumulative_on_ventilator",
    "cumulative_recovered",
    "cumulative_deaths",
    "cumulative_hospitalized",
    "cumulative_test_results",
    "deaths",
    "hospitalized",
    "cases",
    "negative_tests",
    "test_results",
]


class Transform(Node):
    def run(self, df):
        df.rename(
            columns={
                "state": "iso2",
                "positive": "cumulative_cases",
                "positiveIncrease": "cases",
                "negative": "cumulative_negative_tests",
                "negativeIncrease": "negative_tests",
                "pending": "cumulative_pending_tests",
                "totalTestResults": "cumulative_test_results",
                "totalTestResultsIncrease": "test_results",
                "hospitalized": "cumulative_hospitalized",
                "hospitalizedIncrease": "hospitalized",
                "hospitalizedCurrently": "currently_hospitalized",
                "inIcuCurrently": "currently_in_icu",
                "inIcuCumulative": "cumulative_in_icu",
                "onVentilatorCurrently": "currently_on_ventilator",
                "onVentilatorCumulative": "cumulative_on_ventilator",
                "recovered": "cumulative_recovered",
                "death": "cumulative_deaths",
                "deathIncrease": "deaths",
            },
            inplace=True,
        )
        df["date"] = pd.to_datetime(df["date"].astype(str))
        df["iso2"] = "US-" + df["iso2"]
        df = df[KEEP_COLUMNS]

        # Reorder for diff operation
        df.set_index(["iso2", "date"], inplace=True)
        with pd.option_context("mode.chained_assignment", None):
            df.sort_index(inplace=True)

            # Forward fill since some cumulative columns have holes
            for column in df.columns:
                if "cumulative" not in column:
                    continue
                for state_abbr in df.index.levels[0]:
                    df.loc[state_abbr, column] = (
                        df.loc[state_abbr, column].fillna(method="ffill").values
                    )

        # Calculate daily numbers from diff in cumulative
        diff_columns = [
            "cumulative_in_icu",
            "cumulative_on_ventilator",
            "cumulative_recovered",
        ]
        df_diff = df.groupby("iso2").apply(apply_df_diff, diff_columns)
        with pd.option_context("mode.chained_assignment", None):
            df["in_icu"] = df_diff["cumulative_in_icu"]
            df["on_ventilator"] = df_diff["cumulative_on_ventilator"]
            df["recovered"] = df_diff["cumulative_recovered"]
            df.reset_index(inplace=True)
            df.set_index(["date", "iso2"], inplace=True)
            df.sort_index(inplace=True)

        self.push(df)


if __name__ == "__main__":
    glider = Glider(
        DataFrameCSVExtract("extract")
        | Transform("transform")
        | LenPrint("print")
        | DataFrameCSVLoad("load")
    )
    print("Syncing covidtracking.com data")
    glider.consume(URL, load=dict(f=OUTFILE))
