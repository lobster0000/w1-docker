#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os

# df = pd.read_csv('green_tripdata_2019-09.csv', nrows=100)
# print(pd.io.sql.get_schema(df,name='green_taxi_data'))

# pd.to_datetime(df.lpep_pickup_datetime)

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db 
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv.gz'
    os.system(f"wget {url} -O {csv_name}")
    os.system(f"wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv -O taxi+_zone_lookup.csv")
    
 
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    # engine.connect()

    df_iter = pd.read_csv('output.csv.gz', iterator=True, compression="gzip", chunksize=100000)

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)


    df.head(n=0).to_sql(name=table_name, con=engine,if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')
    df_zones = pd.read_csv('taxi+_zone_lookup.csv')
    df_zones.to_sql(name='zones', con=engine, if_exists='replace')
    

    while True:
        t_start = time()
        df = next(df_iter)
        
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
         
        df.to_sql(name=table_name, con=engine, if_exists='append')
        
        t_end = time()
        
        print('insert another chunk..., took %.3f second' %(t_end - t_start)) 
     
        
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of table where we will write results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()
    main(args)

