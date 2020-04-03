#!/usr/bin/env python
"""https://github.com/starschema/COVID-19-data

Only a subset of available datasets are used here, as others are pulled
from the original source.
"""
from glide_covid_19.utils import *


HC_OUTFILE = OUTDIR + "starschema_healthcare_capacity.csv"
ICU_OUTFILE = OUTDIR + "starschema_icu_county.csv"
BASE_URL = "https://s3-us-west-1.amazonaws.com/starschema.covid/"
DATASETS = {
    "US healthcare capacity by state, 2018": BASE_URL + "KFF_HCP_capacity.csv",
    "ICU beds by county, US": BASE_URL + "KFF_US_ICU_BEDS.csv",
}


class TransformHCCapacity(Node):
    def run(self, df):
        df.rename(
            columns={
                "Country/Region": "country_region",
                "State / Province": "state_province",
                "Total Hospital Beds": "hospital_beds",
                "Hospital Beds per 1,000 Population": "hospital_beds_per_1k",
                "Total CHCs": "total_chcs",
                "CHC Service Delivery Sites": "chc_service_delivery_sites",
                "Footnotes": "footnotes",
            },
            inplace=True,
        )
        i = df[df["state_province"].isnull()].index
        df.drop(i, inplace=True)
        df.drop(columns=["footnotes", "hospital_beds_per_1k"], inplace=True)
        # https://en.wikipedia.org/wiki/ISO_3166-1
        df.loc[
            df["country_region"] == "United States", "country_region"
        ] = "United States of America"
        self.push(df)


class TransformICUCounty(Node):
    def run(self, df):
        df.rename(columns=lambda x: x.lower(), inplace=True)
        df["fips"] = df["fips"].astype(str).str.zfill(5)
        df.drop(
            columns=[
                "note",
                "country_region",
                "county",
                "state",
                "iso3166_1",
                "iso3166_2",
            ],
            inplace=True,
        )
        self.push(df)


if __name__ == "__main__":
    hc_capacity = Glider(node_template())
    hc_capacity["transform"] = TransformHCCapacity("transform")

    icu_county = Glider(node_template())
    icu_county["transform"] = TransformICUCounty("transform")

    with get_sqlite_conn() as conn:
        conn.execute("DELETE FROM healthcare_capacity")
        conn.execute("DELETE FROM icu_beds_by_fips")

        hc_capacity.consume(
            DATASETS["US healthcare capacity by state, 2018"],
            csv_load=dict(f=HC_OUTFILE, index=False),
            sql_load=dict(
                table="healthcare_capacity", conn=conn, if_exists="append", index=False
            ),
        )

        icu_county.consume(
            DATASETS["ICU beds by county, US"],
            csv_load=dict(f=ICU_OUTFILE, index=False),
            sql_load=dict(
                table="icu_beds_by_fips", conn=conn, if_exists="append", index=False
            ),
        )

        conn.commit()
