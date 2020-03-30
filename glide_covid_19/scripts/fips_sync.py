#!/usr/bin/env python
from glide_covid_19.utils import *


CENSUS_OUTFILE = OUTDIR + "fips.csv"
CENSUS_URL = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2019_Gazetteer/2019_Gaz_counties_national.zip"
CENSUS_FULL_HEADER = [
    "USPS",  # AL
    "GEOID",  # 01001
    "ANSICODE",  # 00161526
    "NAME",  # Autauga County
    "ALAND",  # 1539602137
    "AWATER",  # 25706961
    "ALAND_SQMI",  # 594.444
    "AWATER_SQMI",  # 9.926
    "INTPTLAT",  # 32.532237
    "INTPTLONG",  # -86.64644
]
CENSUS_HEADER = ["USPS", "NAME", "GEOID", "INTPTLAT", "INTPTLONG"]


class Transform(Node):
    def run(self, df):
        df.columns = df.columns.str.strip()
        df.drop(columns=set(CENSUS_FULL_HEADER) - set(CENSUS_HEADER), inplace=True)
        df.rename(
            columns=dict(
                GEOID="fips",
                NAME="county_name",
                USPS="state_abbr",
                INTPTLAT="lat",
                INTPTLONG="long",
            ),
            inplace=True,
        )
        df["lat"] = df["lat"].round(6)
        df["long"] = df["long"].round(6)
        df.set_index(["fips"], inplace=True)
        df.sort_index(inplace=True)
        self.push(df)


if __name__ == "__main__":
    glider = Glider(FileCopy("download") | ExtractFromZip("unzip") | node_template())
    glider["transform"] = Transform("transform")

    with get_sqlite_conn() as conn:
        conn.execute("DELETE FROM fips")
        glider.consume(
            CENSUS_URL,
            download=dict(f_out="/tmp/2019_Gaz_counties_national.zip"),
            unzip=dict(f_inner="2019_Gaz_counties_national.txt"),
            extract=dict(sep="\t", dtype=dict(GEOID=str)),
            csv_load=dict(f=CENSUS_OUTFILE),
            sql_load=dict(table="fips", conn=conn, if_exists="append"),
        )
        conn.commit()
