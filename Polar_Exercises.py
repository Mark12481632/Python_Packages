# ########################################
# Use Polars to implement solutions to
#   problems provided.
# ########################################

import polars as pl
from typing import Sequence, Any

# Read-in dataset.
custom_schema = {'Id': pl.Int64, 
                 'Name': pl.Utf8,
                 'Date': pl.Utf8, 
                 'Balance': pl.Float64, 
                 'Rating': pl.Float64, 
                 'Active': pl.Utf8
                }
df_orig = pl.read_csv("Sample_Data_Polars.csv",
                      has_header=True,
                      schema_overrides=custom_schema,
                      null_values=["null"])
print(df_orig.head(3))      # See what data looks like
print(df_orig.describe())


# Exercise 1: Filtering + Selecting
#     Filter all rows where:
#
#     Balance > 25,000
#     Rating ≥ 4.0
#     Then select:
#         ID
#         Name
#         Balance
#         Rating
#
# Null Handling:
#
#        Treat nulls in Balance and Rating as not meeting 
#        the filter (i.e., exclude them).

# Remove null values (for some reason - nulls are "null")

df = df_orig.filter((pl.col("Balance").is_not_null()) & (pl.col("Balance") > 25000.0)
                    & (pl.col("Rating").is_not_null()) & (pl.col("Rating") >= 4.0))

print(df.head(10))


# Exercise 2: Grouping & Aggregation
#      Group by "Active" status:
#
#         Compute total Balance.
#         Compute average Rating (skip null values automatically).
#         Count total records per "Active" group.

df = df_orig.group_by("Active").agg(
                 pl.sum("Balance"),
                 pl.mean("Rating"),
                 pl.count().alias("Count")
         )

print(df)


# Exercise 3: Chained Filtering
#     Apply these filters (in sequence):
#
#         Keep only non-null "Active".
#         Filter "Active" == "True".
#         Filter Balance > 30,000.
#         Select:
#
#             ID
#             Name
#             Balance

df = df_orig.filter((pl.col("Active").is_not_null()) & (pl.col("Balance") > 30000.0)) \
        .select(pl.col("Id"), pl.col("Name"), pl.col("Balance"))

print(df.head(10))

# Exercise 4: map_rows()
#     Classify each record:
#
#         "High" → Balance > 40,000.
#         "Mid" → 20,000 < Balance ≤ 40,000.
#         "Low" → Otherwise.
#         "Unknown" → If Balance is null.
#         Add a new "Balance_Class" column using map_rows().

def classify_balance(row: Sequence[Any]) -> str:
    """
    """
    if row[3] is None:
        return "Unknown"
    elif row[3] > 40000:
        return "High"
    elif row[3] <= 20000:
        return "Low"
    else:
        return "Mid"

df = df_orig.with_columns(
            pl.Series(df_orig.map_rows(classify_balance),
                      dtype=pl.Utf8) \
                .alias("classified_balance")
    )

print(df.head(10))
