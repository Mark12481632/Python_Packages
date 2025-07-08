# #################################
# Getting started with Polars (3)
# #################################

import polars as pl
#from typing import Sequence, Any

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

# Looking into aggregation and grouping.

# Sum accross a column:
print(
    df.select(pl.sum("dlbytes").alias("Total_dlbytes"),
              pl.min("dlbytes").alias("Min_dlbytes"),
              pl.max("dlbytes").alias("Max_dlbytes"),
              pl.std("dlbytes").alias("Std_Dev_dlbytes"),
              pl.mean("dlbytes").alias("Mean_dlbytes"),
              pl.median("dlbytes").alias("Median_dlbytes"),
              pl.first("dlbytes").alias("First_dlbytes"))
)

# With "lazy()" nothing executed until "collect()" invoked.
# Here we aggregate total "dlbytes" by country.
query = (
    df.lazy()
        .group_by("country")
            .agg(pl.sum("dlbytes").alias("sum_dl_bytes"))
)
print(query.collect())

# ==================================================

# Group details by platform.
#   for each platform:
#      - Number of rows for that platform.
#      - Total number of dlbytes
#      - List of all "userid"s for that platform.
#      - "HTTP_Method" in 1st record for that platform
query = (
    df.lazy()
        .group_by("platform")
            .agg(pl.count().alias("row_cnt"),
                 pl.sum("dlbytes"),
                 pl.col("userid"),
                 pl.first("http_method"))
                 .sort("row_cnt", descending=True)
)
print(query.collect())

# "Sliding Window" functionality.
result = df.sort("date_time", descending=True).select(
                pl.col("date_time"),
                pl.col("dlbytes"),
                pl.col("dlbytes").rolling_max(window_size=3).alias("Max_dlbytes"),
                pl.col("dlbytes").rolling_min(window_size=3).alias("Min_dlbytes")
        )

print(result.head(16))

print(
    df.select(pl.col("country"),
              pl.col("userid"),
              pl.col("dlbytes"),
              pl.col("dlbytes").min().over("country").alias("min_cntry_dlbytes")).head(7)
)



# Filtering:
fltered = df.select(
            pl.col("platform"),
            pl.col("dlbytes"),
        ).filter(
            pl.col("dlbytes") > 100000
        )

print(fltered.head(5))

print(df.filter(pl.col("http_method") == "HTTP").head(6))
