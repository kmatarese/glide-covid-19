"""https://github.com/nytimes/covid-19-data"""

import zipfile

from glide import *
from glide.extensions.pandas import *


# date,county,state,fips,cases,deaths
# 2020-01-21,Snohomish,Washington,53061,1,0
NYT_URL = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"

CENSUS_URL = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2019_Gazetteer/2019_Gaz_counties_national.zip"
CENSUS_FULL_HEADER = [
    "USPS", # AL
    "GEOID", # 01001
    "ANSICODE", # 00161526
    "NAME", # Autauga County
    "ALAND", # 1539602137
    "AWATER", # 25706961
    "ALAND_SQMI", # 594.444
    "AWATER_SQMI", # 9.926
    "INTPTLAT", # 32.532237
    "INTPTLONG" # -86.64644
]
CENSUS_HEADER = ["USPS", "GEOID", "INTPTLAT", "INTPTLONG"]


class ExtractFromZip(Node):
    def run(self, f_zip, f_inner):
        zf = zipfile.ZipFile(f_zip)
        with zf.open(f_inner, 'r') as infile:
            self.push(infile)


class CleanCensusData(Node):
    def run(self, df):
        df.columns = df.columns.str.strip()
        df.drop(
            columns=set(CENSUS_FULL_HEADER) - set(CENSUS_HEADER),
            inplace=True
        )
        df.rename(
            columns=dict(
                USPS="state_abbr",
                GEOID="fips",
                INTPTLAT="lat",
                INTPTLONG="long"
            ),
            inplace=True
        )
        df["lat"] = df["lat"].round(6)
        df["long"] = df["long"].round(6)
        df.set_index(["fips"], inplace=True)
        self.push(df)


class CleanNYTData(Node):
    def run(self, df):
        df.set_index(["date", "fips"], inplace=True)
        self.push(df)


census = Glider(
    FileCopy("download")
    | ExtractFromZip("unzip")
    | DataFrameCSVExtract("to_csv")
    | CleanCensusData("clean")
    | DataFrameCSVLoad("load") # Save unjoined data
    | Return("return", flatten=True)
)

nyt = Glider(
    DataFrameCSVExtract("csv")
    | CleanNYTData("clean")
    | DataFrameCSVLoad("load") # Save unjoined data
    | Return("return", flatten=True)
)

loader = Glider(
    Join("join", on=["fips"], how="left")
    | LenPrint("print")
    | DataFrameCSVLoad("load")
)

census_data = census.consume(
    CENSUS_URL,
    download=dict(f_out="/tmp/2019_Gaz_counties_national.zip"),
    unzip=dict(f_inner="2019_Gaz_counties_national.txt"),
    to_csv=dict(sep="\t", dtype=dict(GEOID=str)),
    load=dict(f="/tmp/census_counties.csv")
)

nyt_data = nyt.consume(
    NYT_URL,
    csv=dict(dtype=dict(fips=str)),
    load=dict(f="/tmp/nytimes_covid_timeseries.csv")
)

# Output format: date,fips,county,state,cases,deaths,state_abbr,lat,long
loader.consume(
    [[nyt_data, census_data]],
    load=dict(f="/tmp/nytimes_covid_timeseries_joined.csv")
)
