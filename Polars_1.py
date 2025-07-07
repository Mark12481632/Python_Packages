# #################################
# Getting started with Polars
# #################################

import polars as pl

# 1. Read-in data:
df = pl.read_csv("course_file.csv", has_header=True) # Indicate 1st line of file is header.
print(df.head(3))                                    # Show 1st 3 lines
print(df.tail(2))                                    # Show last 2 lines
print(f"No. Rows:{len(df)}")                         # Number of records in data frame.
print(f"Columns:{df.columns}")                       # Columns in DF.
print(f".. Associated data types:{df.dtypes}")       # Type of each column.
print(f"Shape of Data Frame:{df.shape}")             # Number of rows and number columns

print("========================")
# Summary data
print(df.describe())

