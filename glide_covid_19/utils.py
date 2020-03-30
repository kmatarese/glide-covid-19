import os
import sqlite3
import zipfile

from glide import *
from glide.extensions.pandas import *


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


class ExtractFromZip(Node):
    def run(self, f_zip, f_inner):
        zf = zipfile.ZipFile(f_zip)
        with zf.open(f_inner, "r") as infile:
            self.push(infile)
