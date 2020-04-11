#!/usr/bin/env python
"""https://github.com/CSSEGISandData/COVID-19"""

from glide_covid_19.utils import *


OUTFILE = OUTDIR + "jhu_covid_timeseries.csv"

BASE_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/"
DATASETS = {
    "Confirmed": BASE_URL + "time_series_covid19_confirmed_global.csv",
    "Deaths": BASE_URL + "time_series_covid19_deaths_global.csv",
}
SOURCE_INDEX = ["Date", "Country/Region", "Province/State", "Lat", "Long"]
GROUP_INDEX = ["country_region", "state_province", "date"]
JOIN_INDEX = ["date", "country_region", "state_province"]
TARGET_INDEX = ["date", "iso1", "iso2"]
VALUE_RENAMES = dict(Confirmed="cumulative_cases", Deaths="cumulative_deaths")
UNKNOWN_STATE = "Unknown"

iso1_df = get_iso1_geos_df()
iso2_df = get_iso2_geos_df()


class Transform(Node):
    def run(self, df, value_name):
        df.drop(columns=["Lat", "Long"], inplace=True)

        df["Date"] = pd.to_datetime(df["Date"])
        df["Date"] = df["Date"].dt.date

        df["Province/State"].fillna(UNKNOWN_STATE, inplace=True)

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

        # Calculate daily numbers from diff in cumulative
        df.set_index(GROUP_INDEX, inplace=True)
        df.sort_index(inplace=True)
        df_diff = df.groupby(["country_region", "state_province"]).apply(
            apply_df_diff, [value_rename]
        )
        non_cumulative_name = value_rename.replace("cumulative_", "")
        df[non_cumulative_name] = df_diff[value_rename]

        df.reset_index(inplace=True)
        df.set_index(JOIN_INDEX, inplace=True)
        df.sort_index(inplace=True)

        self.push(df)


class ToISOGeos(Node):
    def run(self, df):
        df.reset_index(inplace=True)

        country_remap = {
            "Bahamas": "The Bahamas",
            "Burma": "Myanmar",
            "Cabo Verde": "Cape Verde",
            "Congo (Brazzaville)": "Congo-Brazzaville",
            "Congo (Kinshasa)": "Democratic Republic of the Congo",
            "Cote d'Ivoire": "Côte d'Ivoire",
            "Gambia": "The Gambia",
            "Korea, South": "South Korea",
            "Namibia": "Namibia",
            "Sao Tome and Principe": "São Tomé and Príncipe",
            "Taiwan*": "Taiwan",
            "Timor-Leste": "East Timor",
            "West Bank and Gaza": "Palestinian Territories",
            "Western Sahara": "Sahrawi Arab Democratic Republic",
            "US": "United States",
            # Ignoring these:
            # Diamond Princess
            # Holy See
            # MS Zaandam
        }

        for old, new in country_remap.items():
            df.loc[df["country_region"] == old, "country_region"] = new

        df = df.merge(iso1_df, how="left", left_on="country_region", right_on="name")
        df.drop(columns=["name"], inplace=True)
        df.drop(df.loc[df["iso1"].isnull()].index, inplace=True)

        df = df.merge(
            iso2_df,
            how="left",
            left_on=["iso1", "state_province"],
            right_on=["iso1", "name"],
        )
        df.loc[df["state_province"] == UNKNOWN_STATE, "iso2"] = UNKNOWN_STATE
        # Note: The state-level data is quite incomplete, so we leave the iso1
        # geo code in here too.
        df.drop(columns=["country_region", "state_province", "name"], inplace=True)

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
        Join("join", on=JOIN_INDEX)
        | ToISOGeos("to_iso")
        | LenPrint("print")
        | DataFrameCSVLoad("load")
    )

    print("Syncing JHU data")

    cleaned = []
    for name, url in DATASETS.items():
        rv = cleaner.consume(
            url, melt=dict(value_name=name), transform=dict(value_name=name)
        )
        cleaned.append(rv)

    loader.consume([cleaned], load=dict(f=OUTFILE))
