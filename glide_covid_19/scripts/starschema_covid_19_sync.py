"""https://github.com/starschema/COVID-19-data

Only a subset of available datasets are used here, as others are pulled
from the original source.
"""

import os

from glide import *
from glide.extensions.pandas import *


OUTDIR = os.path.dirname(os.path.abspath(__file__)) + "/../data/"
HC_OUTFILE = OUTDIR + "starschema_healthcare_capacity.csv"
ICU_OUTFILE = OUTDIR + "starschema_icu_county.csv"

BASE_URL = "https://s3-us-west-1.amazonaws.com/starschema.covid/"
DATASETS = {
    # "US COVID-19 testing and mortality": BASE_URL + "CT_US_COVID_TESTS.csv",
    # Note: Same as other COVID Tracking Data
    # "Global data on healthcare providers": BASE_URL + "HS_BULK_DATA.csv",
    # Note: Not reliably populated
    # Columns:
    # Long, Lat, Healthcare_Provider_Type, Name, Operator, Beds, Staff_Medical,
    # Staff_Nursing
    # "Global case counts": BASE_URL + "JHU_COVID-19.csv",
    # Note: Same as other JHU Data
    "US healthcare capacity by state, 2018": BASE_URL + "KFF_HCP_capacity.csv",
    # Columns:
    # Country/Region, State / Province, Total Hospital Beds, Hospital Beds per
    # 1,000 Population, Total CHCs, CHC Service Delivery Sites, Footnotes
    # "US policy actions by state": BASE_URL + "KFF_US_POLICY_ACTIONS.csv",
    # Columns:
    # Location, Waive Cost Sharing for COVID-19 Treatment, Free Cost Vaccine
    # When Available, State Requires Waiver of Prior Authorization
    # Requirements, Early Prescription Refills, Marketplace Special Enrollment
    # Period (SEP), Section 1135 Waiver, Paid Sick Leave, Notes,
    # Last_Update_Date
    # "US actions to mitigate spread, by state": BASE_URL + "KFF_US_STATE_MITIGATIONS.csv",
    # Columns:
    # Location, Bar/Restaurant Limits, Mandatory Quarantine, Non-Essential
    # Business Closures, Emergency Declaration, Primary Election Postponement,
    # State-Mandated School Closures, Large Gatherings Ban, Last_Update_Date
    "ICU beds by county, US": BASE_URL + "KFF_US_ICU_BEDS.csv",
    # Columns:
    # COUNTRY_REGION, FIPS, COUNTY, STATE, ISO3166_1, ISO3166_2, HOSPITALS,
    # ICU_BEDS, NOTE
    # "Italy case statistics, detailed": BASE_URL + "PCM_DPS_COVID19-DETAILS.csv",
    # Columns:
    # Country/Region, Province/State, Date, Hospitalized, Intensive_Care,
    # Total_Hospitalized, Home_Isolation, Total_Positive, New_Positive,
    # Discharged_Healed, Deceased, Total_Cases, Tested,
    # Hospitalized_Since_Prev_Day, Intensive_Care_Since_Prev_Day,
    # Total_Hospitalized_Since_Prev_Day, Home_Isolation_Since_Prev_Day,
    # Total_Positive_Since_Prev_Day, Discharged_Healed_Since_Prev_Day,
    # Deceased_Since_Prev_Day, Total_Cases_Since_Prev_Day,
    # Tested_Since_Prev_Day, ISO3166_1, ISO3166_2, Note_IT, Note_EN
    # "WHO situation reports": BASE_URL + "WHO_SITUATION_REPORTS.csv",
    # Columns:
    # Country, Total_Cases, Cases_New, Deaths, Deaths_New,
    # Transmission_Classification, Days_Since_Last_Reported_Case, ISO3166-1,
    # Country/Region, Date, Situation_Report_name, Situation_Report_URL,
    # Last_Update_Date
    # "US case and mortality counts, by county": BASE_URL + "NYT_US_COVID19.csv",
    # Note: Same as other NYT Data
    # "COVID-19 cases and deaths, Canada, province level": BASE_URL + "NVH_CAN_DETAILED.csv",
}


class CleanHCCapacityData(Node):
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
        df.drop(columns=["footnotes"], inplace=True)
        self.push(df)


class CleanICUCountyData(Node):
    def run(self, df):
        df.rename(columns=lambda x: x.lower(), inplace=True)
        df["fips"] = df["fips"].astype(str).str.zfill(5)
        df.drop(columns=["note"], inplace=True)
        self.push(df)


hc_capacity = Glider(
    DataFrameCSVExtract("extract")
    | CleanHCCapacityData("clean")
    | DataFrameCSVLoad("load", index=False)
)

icu_county = Glider(
    DataFrameCSVExtract("extract")
    | CleanICUCountyData("clean")
    | DataFrameCSVLoad("load", index=False)
)

hc_capacity.consume(
    DATASETS["US healthcare capacity by state, 2018"], load=dict(f=HC_OUTFILE)
)

icu_county.consume(DATASETS["ICU beds by county, US"], load=dict(f=ICU_OUTFILE))
