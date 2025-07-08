# #################################
# Getting started with Polars (1)
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

# =============================================================================
# Change datatypes for the columns.
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
print(df.head(3))                            # Show 1st 3 lines - userid now string

print("----------------------------------------------")

# Remove rows where date-time is null AND user-id is null.
# Replace by | if we want OR (instead of AND)
df_no_nulls = df.filter((df['date_time'].is_not_null()) & (df['userid'].is_not_null()))
print(df_no_nulls.head(2))

# We can also drop records with nulls in any of 3 given columns:
df_no_nulls2 = df.drop_nulls(subset=['date_time', 'userid', 'domain'])
print(df_no_nulls2.head(3))

print("+++++++++++++++++++++++++++++++++++++++++++")

# We can also treat/substitute null values:
print(df.with_columns(
         pl.col('userid').fill_null(pl.lit('22')),          # When userid is null replace with 22
         pl.col('dlbytes').fill_null(pl.median('dlbytes')) # When dlbytes is null replace with median value
     ).head(5)
)

df.fill_nan(344) # Fill any NaN values with 344.

# ##################################################
# Selecting columns.
# Select all columns except dlbytes/ulbytes
print(df.select(pl.col("*").exclude('dlbytes', 'ulbytes')).head(2))

# Select only columns dlbytes/ulbytes
print(df.select(pl.col("dlbytes"), pl.col("ulbytes")).head(2))

# Silly, but select all columns
print(df.select(pl.all()).head(1))

# =============================================================================
# Calculations on data...
print(df.select(pl.col('dlbytes'), (pl.col('dlbytes')*5).alias('dlbytes_5')).head(6))


print(
    df.select(
        pl.col('dlbytes'), 
        (pl.col('dlbytes')*5).alias('dlbytes_5'),
        pl.col('ulbytes'), 
        (pl.col('ulbytes')*5).alias('ulbytes_5'),
        (pl.col('dlbytes') + pl.col('ulbytes')).alias('Total'),
        (pl.col('dlbytes') / pl.col('ulbytes')).cast(pl.Float64).round(2).alias('fraction')
    ).head(6)
)

# Conditionals .....
print(df.select(
        pl.col('dlbytes'),
        pl.when(pl.col('dlbytes') > 500000)
            .then(pl.lit('large'))
            .otherwise(pl.lit('small'))
            .alias("When_Test")
     ).head(3)
)

print(df.select(
        pl.col('country'),
        pl.when(pl.col('country') == 'England')
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("Is_England")
     ).head(3)
)

countries_of_interest = ['Scotland', 'Wales']
print(df.select(
        pl.col('country'),
        pl.when(pl.col('country').is_in(countries_of_interest))
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("Minor_Countries")
     ).head(5)
)