import os
import sqlite3
import zipfile

from glide import *
from glide.extensions.pandas import *
import pandas as pd


OUTDIR = os.path.dirname(os.path.abspath(__file__)) + "/data/"
SQLITE_DB_FILE = OUTDIR + "sqlite.db"


# A simple template to cover some common cases
node_template = NodeTemplate(
    DataFrameCSVExtract("extract")
    | PlaceholderNode("transform")
    | LenPrint("print")
    | [DataFrameCSVLoad("csv_load"), DataFrameSQLLoad("sql_load")]
)


def get_sqlite_conn():
    return sqlite3.connect(SQLITE_DB_FILE)


def apply_df_diff(df, columns):
    first_values = {}

    for column in columns:
        # These are expected to be cumulative columns, so forward filling
        # with the previous value is assumed to be safe.
        df[column] = df[column].fillna(method="ffill")
        first_index = df[column].first_valid_index()
        if first_index is None:
            # This will happen if all values=NaN
            continue
        first_value = df[column].loc[first_index]
        first_values[column] = (first_index, first_value)

    df = df.diff()

    # Add in the starting value, since diff() doesn't do that
    for column in columns:
        if column not in first_values:
            continue
        first_index, first_value = first_values[column]
        df.loc[first_index, column] = first_value

    return df


def get_iso1_geos_df():
    return pd.read_csv(OUTDIR + "iso1.csv", usecols=["iso1", "name"])


def get_iso2_geos_df():
    return pd.read_csv(OUTDIR + "iso2.csv", usecols=["iso2", "iso1", "name"])


def get_fips_df():
    return pd.read_csv(OUTDIR + "fips.csv", usecols=["fips", "name", "state_code_iso"])


class ExtractFromZip(Node):
    def run(self, f_zip, f_inner):
        zf = zipfile.ZipFile(f_zip)
        with zf.open(f_inner, "r") as infile:
            self.push(infile)


class DataFrameTransform(Node):
    def run(self, df, drop=None, keep=None, rename=None, new=None):
        assert not (drop and keep), "Only one of drop and keep may be specified"
        if rename:
            df.rename(columns=rename, inplace=True)
        if drop:
            df.drop(columns=drop, inplace=True)
        if keep:
            df = df[keep]
        if new:
            with pd.option_context("mode.chained_assignment", None):
                for column, func in new.items():
                    df[column] = func(df)
        self.push(df)
