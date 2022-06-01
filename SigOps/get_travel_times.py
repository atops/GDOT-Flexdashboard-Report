# -*- coding: utf-8 -*-
"""
Created on Mon Sep 24 20:42:51 2018

@author: Alan.Toppen
"""


import sys
import pandas as pd
import numpy as np
import requests
import uuid
import polling
import time
import yaml
from datetime import datetime, timedelta
import pytz
from zipfile import ZipFile
import json
import io
import boto3
import dask.dataframe as dd

s3 = boto3.client('s3')


def is_success(response):
    try:
        x = json.loads(response.content.decode('utf-8'))
        return('state' in x.keys() and x['state']=='SUCCEEDED')
    except:
        return False


def get_tmc_data(start_date, end_date, tmcs, key, dow=[2,3,4], bin_minutes=60, initial_sleep_sec=0):

    # Allow sleep time to space out requests when running in a loop
    time.sleep(initial_sleep_sec)

    uri = 'http://kestrel.ritis.org:8080/{}'

    #----------------------------------------------------------  
    payload = {
        "dates": [
            {
                "end": end_date,
                "start": start_date
            }
        ],
        "dow": dow,
        "dsFields": [
            {
                "columns": [
                    "SPEED",
                    "REFERENCE_SPEED",
                    "TRAVEL_TIME_MINUTES",
                    "CONFIDENCE_SCORE"
                ],
                "dataSource": "vpp_here",
                "qualityFilter": {
                    "max": 1,
                    "min": 0,
                    "thresholds": [
                        30,
                        20,
                        10
                    ]
                }
            }
        ],
        "granularity": {
            "type": "minutes",
            "value": bin_minutes
        },
        "times": [
            {
                "end": None,
                "start": "00:00:00.000"
            }
        ],
        "tmcs": tmcs,
        "travelTimeUnits": "MINUTES",
        "uuid": str(uuid.uuid1())
    }  
    
    response = requests.post(uri.format('jobs/export'), 
                             params = {'key': key}, 
                             json = payload)
    print('travel times response status code:', response.status_code)
    
    if response.status_code == 200: # Only if successful response

        # retry at intervals of 'step' until results return (status code = 200)
        jobid = json.loads(response.content.decode('utf-8'))['id']

        polling.poll(
            lambda: requests.get(uri.format('jobs/status'), params = {'key': key, 'jobId': jobid}),
            check_success = is_success,
            step=30,
            timeout=600)

        results = requests.get(uri.format('jobs/export/results'), 
                               params = {'key': key, 'uuid': payload['uuid']})
        print('travel times results received')
        
        # Save results (binary zip file with one csv)
        with io.BytesIO() as f:
            f.write(results.content)
            f.seek(0)
            with ZipFile(f, 'r') as zf:
                df = pd.read_csv(zf.open('Readings.csv'))

    else:
        df = pd.DataFrame()

    print('{} travel times records'.format(len(df)))
        
    return df


def get_travel_times(cred, conf, tt_conf):

    # start_date is either the given day or the first day of the month
    start_date = conf['start_date']
    if start_date == 'yesterday':
        bucket = conf['bucket']
        suff = tt_conf['table_suffix']
        print(f's3://{bucket}/mark/travel_times_{suff}/*/*')
        last_date = max(dd.read_parquet(f's3://{bucket}/mark/travel_times_{suff}')['date']
                        .drop_duplicates()
                        .compute()
                        .values)
        start_date = pd.Timestamp(last_date) + timedelta(days=1)
        # start_date = (datetime.now(pytz.timezone('America/New_York')) - timedelta(days=7))
    start_date = start_date.strftime('%Y-%m-%d')

    # end date is either the given date + 1 or today. 
    # end_date is not included in the query results
    end_date = conf['end_date']
    if end_date == 'yesterday': 
        end_date = datetime.now(pytz.timezone('America/New_York')) - timedelta(days=1)
    end_date = (end_date + timedelta(days=1)).strftime('%Y-%m-%d')

    # start_date = '2021-09-01'
    # end_date = '2021-09-30'
    
    tmc_df = (pd.read_excel(f"s3://{conf['bucket']}/{conf['corridors_TMCs_filename_s3']}", 
                            engine='openpyxl')
                .rename(columns={'length': 'miles'})
                .fillna(value={'Corridor': 'None', 'Subcorridor': 'None'}))

    tmc_df = tmc_df[tmc_df.Corridor != 'None']
    tmc_list = list(set(tmc_df.tmc.values))

    print(f'travel times: {start_date} - {end_date}')

    try:
        tt_df = get_tmc_data(
            start_date, 
            end_date, 
            tmc_list, 
            cred['RITIS_KEY'], 
            dow=tt_conf['dow'], 
            bin_minutes=tt_conf['bin_minutes'], 
            initial_sleep_sec=0
        )

    except Exception as e:
        print('ERROR retrieving tmc records')
        print(e)
        tt_df = pd.DataFrame()
                                                                                                                                                
    finally:
        return tt_df


def upload_travel_times_to_s3(df, conf, tt_conf):

    def uf(df): # upload parquet file
        date_string = df.date.values[0]
        bucket = conf['bucket']
        interval = tt_conf['table_suffix']
        filename = f'travel_times_{date_string}.parquet'
        df.drop(columns=['date'])\
            .to_parquet(f's3://{bucket}/mark/travel_times_{interval}/date={date_string}/{filename}')

    df.to_parquet('travel_times.parquet')

    # Write to parquet files and upload to S3
    df = df.reset_index()
    df['measurement_tstamp'] = pd.to_datetime(df.measurement_tstamp)
    df['date'] = df.measurement_tstamp.dt.date
    df.groupby(['date']).apply(uf)


if __name__=='__main__':

    with open(sys.argv[1]) as yaml_file:
        tt_conf = yaml.load(yaml_file, Loader=yaml.Loader)

    with open('Monthly_Report_AWS.yaml') as yaml_file:
        cred = yaml.load(yaml_file, Loader=yaml.Loader)

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    tt_df = get_travel_times(cred, conf, tt_conf)
    if not tt_df.empty:
        upload_travel_times_to_s3(tt_df, conf, tt_conf)
