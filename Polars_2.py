# #################################
# Getting started with Polars (2)
# #################################

import polars as pl

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
                 dtypes=custom_schema)