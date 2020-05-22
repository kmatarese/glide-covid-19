#!/usr/bin/env python
"""https://github.com/hyperknot/country-levels"""

from glide_covid_19.utils import *

ISO1_URL = (
    "https://raw.githubusercontent.com/hyperknot/country-levels-export/master/iso1.json"
)
ISO2_URL = (
    "https://raw.githubusercontent.com/hyperknot/country-levels-export/master/iso2.json"
)
FIPS_URL = (
    "https://raw.githubusercontent.com/hyperknot/country-levels-export/master/fips.json"
)

ISO1_OUTFILE = OUTDIR + "iso1.csv"
ISO2_OUTFILE = OUTDIR + "iso2.csv"
FIPS_OUTFILE = OUTDIR + "fips.csv"


class ToRows(Node):
    def run(self, data):
        rows = [v for v in data.values()]
        self.push(rows)


if __name__ == "__main__":
    glider = Glider(
        URLExtract("extract", data_type="json")
        | ToRows("to_rows")
        | ToDataFrame("to_df")
        | DataFrameTransform(
            "transform", rename=dict(center_lat="lat", center_lon="long")
        )
        | LenPrint("print")
        | [DataFrameCSVLoad("csv_load", index=False), DataFrameSQLLoad("sql_load")]
    )

    with get_sqlite_conn() as conn:
        print("Syncing FIPS")
        conn.execute("DELETE FROM fips")
        glider.consume(
            FIPS_URL,
            csv_load=dict(f=FIPS_OUTFILE),
            transform=dict(
                keep=[
                    "fips",
                    "county_code",
                    "name",
                    "name_long",
                    "population",
                    "state_code_int",
                    "state_code_iso",
                    "state_code_postal",
                    "timezone",
                    "lat",
                    "long",
                ]
            ),
            sql_load=dict(table="fips", conn=conn, if_exists="append", index=False),
        )

        print("Syncing ISO1 Geos")
        conn.execute("DELETE FROM iso1_geos")
        glider.consume(
            ISO1_URL,
            csv_load=dict(f=ISO1_OUTFILE),
            transform=dict(
                keep=[
                    "iso1",
                    "admin_level",
                    "name",
                    "osm_id",
                    "population",
                    "timezone",
                    "lat",
                    "long",
                    "wikidata_id",
                    "wikipedia_id",
                ]
            ),
            sql_load=dict(
                table="iso1_geos", conn=conn, if_exists="append", index=False
            ),
        )

        print("Syncing ISO2 Geos")
        conn.execute("DELETE FROM iso2_geos")

        def get_iso1(df):
            return df["iso2"].str.split("-", expand=True)[0]

        glider.consume(
            ISO2_URL,
            transform=dict(
                keep=[
                    "iso2",
                    "admin_level",
                    "name",
                    "osm_id",
                    "population",
                    "timezone",
                    "lat",
                    "long",
                    "wikidata_id",
                    "wikipedia_id",
                ],
                new=dict(iso1=get_iso1),
            ),
            csv_load=dict(f=ISO2_OUTFILE),
            sql_load=dict(
                table="iso2_geos", conn=conn, if_exists="append", index=False
            ),
        )

        conn.commit()
