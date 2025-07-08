# #################################
# Getting started with Polars (2)
# #################################

import polars as pl
from typing import Sequence, Any

custom_schema = {'date_time': pl.Utf8, 
                 'userid': pl.Utf8,      # not the polar implied type
                 'domain': pl.Utf8, 
                 'dlbytes': pl.Int64, 
                 'ulbytes': pl.Int64, 
                 'clientip': pl.Utf8, 
                 'serverip': pl.Utf8, 
                 'country': pl.Utf8, 
                 'txn_time': pl.Float64, 
                 'http_method': pl.Utf8, 
                 'user_agent': pl.Utf8, 
                 'platform': pl.Utf8
                }

df = pl.read_csv("course_file.csv", 
                 has_header=True,
                 schema_overrides=custom_schema)

# Correcting field type - using .with_columns
df = df.with_columns(
    pl.col("date_time").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
)

print(df.head(5))

# ==============================================
platforms = ["Android", "Linux"]

print(df.select(pl.col("platform"), 
                pl.col("dlbytes"),
                pl.when((pl.col("platform").is_in(platforms)) & (pl.col("dlbytes") > 5000))
                    .then(pl.lit(1))
                    .otherwise(pl.lit(0))
                    .alias("platform_when")
    ).head(5)
)

print("++++++++++++++++++++++++++++++++++++++++++++")

print(df.select(pl.col("platform"), 
                pl.col("dlbytes"),
                pl.when((pl.col("ulbytes") > 500000) & (pl.col("dlbytes") > 500000))
                    .then(pl.lit("Large"))
                    .when((pl.col("ulbytes") > 250000) & (pl.col("dlbytes") > 250000))
                    .then(pl.lit("Medium"))
                    .otherwise(pl.lit("Small"))
                    .alias("Tans Size")
    ).head(5)
)

# User defined function:
def calculate_bytes(row: Sequence[Any]) -> int:
    """
    Looking at the data frame dlbytes has index 3 and
    ulbytes has index 4.  Note indexes start at 0 (not 1).
    """
    try:
        dlbytes = int(row[3])      # Use row INDEX not name.
        ulbytes = int(row[4])      # Use row INDEX not name.
        return dlbytes + ulbytes
    except (IndexError, TypeError):
        return -1

# The pl.Seies() is used as it represent a column (all data types the same).
# Similar to Pandas "series".
# NOTE: We CAN'T do this in the .select().
print(
    df.with_columns(
        pl.Series(df.map_rows(calculate_bytes)).alias("bytes_total")
    ).select(pl.col('dlbytes'), pl.col('ulbytes'), pl.col('bytes_total')).head(5)
)