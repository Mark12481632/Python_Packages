# ########################################
# Use Polars to implement solutions to
#   problems provided.
# ########################################

import polars as pl

# Read-in dataset.  Note, empty strings represent nulls.
df_orig = pl.read_csv("Sample_Data_Polars.csv",
                      has_header=True)
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
#        Treat nulls in Balance and Rating as not meeting the filter (i.e., exclude them).

# Exercise 2: Grouping & Aggregation
#      Group by "Active" status:
#
#         Compute total Balance.
#         Compute average Rating (skip null values automatically).
#         Count total records per "Active" group.



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


# Exercise 4: map_rows()
#     Classify each record:
#
#         "High" → Balance > 40,000.
#         "Mid" → 20,000 < Balance ≤ 40,000.
#         "Low" → Otherwise.
#         "Unknown" → If Balance is null.
#         Add a new "Balance_Class" column using map_rows().