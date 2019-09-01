# -*- coding: utf-8 -*-
"""
Created on Mon Sep 24 20:42:51 2018

@author: Alan.Toppen
"""


import os
import pandas as pd
import requests
import uuid
import polling
import time
import yaml
from datetime import datetime, timedelta
import zipfile
import io
import feather
import boto3

os.environ['TZ'] = 'America/New_York'
time.tzset()

s3 = boto3.client('s3')

pd.options.display.max_columns = 10

def get_tmc_data(start_date, end_date, tmcs, key, initial_sleep_sec=0):
    
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
      "dow": [
        2,
        3,
        4
      ],
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
        "value": 60
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
    #----------------------------------------------------------    
    response = requests.post(uri.format('jobs/export'), 
                             params = {'key': key}, 
                             json = payload)
    print('response status code:', response.status_code)
    
    if response.status_code == 200: # Only if successful response

        # retry at intervals of 'step' until results return (status code = 200)
        results = polling.poll(
                lambda: requests.get(uri.format('jobs/export/results'), 
                               params = {'key': key, 'uuid': payload['uuid']}),
                check_success = lambda x: x.status_code == 200 and len(x.content) > 0,
                step = 1,
                timeout = 300
                #poll_forever = True
                )
        print('results received')
        
        # Save results (binary zip file with one csv)
        with io.BytesIO() as f:
            f.write(results.content)
            f.seek(0)
            df = pd.read_csv(f, compression='zip')       

    else:
        df = pd.DataFrame()

    print('{} records'.format(len(df)))
    
    return df

if __name__=='__main__':

    with open('Monthly_Report_AWS.yaml') as yaml_file:
        cred = yaml.load(yaml_file, Loader=yaml.Loader)

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    # start_date is either the given day or the first day of the month
    start_date = conf['start_date']
    if start_date == 'yesterday': 
        start_date = datetime.today() - timedelta(days=1)
    start_date = (start_date - timedelta(days=(start_date.day - 1))).strftime('%Y-%m-%d')

    # end date is either the given date + 1 or today. 
    # end_date is not included in the query results
    end_date = conf['end_date']
    if end_date == 'yesterday': 
        end_date = datetime.today() - timedelta(days=1)
    end_date = (end_date + timedelta(days=1)).strftime('%Y-%m-%d')
    
    s3.download_file(Bucket = 'gdot-spm',
                     Key = 'tmc_routes.feather',
                     Filename = 'tmc_routes.feather')
    tmc_fn = 'tmc_routes.feather'
    tmc_df = feather.read_dataframe(tmc_fn)
    tmc_dict = tmc_df.groupby(['Corridor'])['tmc'].apply(list).to_dict()
    tmc_list = list(set(tmc_df.tmc.values))
    
    #start_date = '2019-02-01'
    #end_date = '2019-03-31'
    
    try:
    
        tt_df = get_tmc_data(start_date, end_date, tmc_list, cred['RITIS_KEY'], 0)

    except Exception as e:
        print('error retrieving records')
        print(e)
        tt_df = pd.DataFrame()
        
        
    if len(tt_df) > 0:
        df = (pd.merge(tmc_df[['tmc', 'miles', 'Corridor']], tt_df, left_on=['tmc'], right_on=['tmc_code'])
            .drop(columns=['tmc'])
            .sort_values(['Corridor', 'tmc_code', 'measurement_tstamp']))

        if len(df) > 0:
            df['reference_minutes'] = df['miles'] / df['reference_speed'] * 60
            df = (df.reset_index(drop=True)
                    .assign(measurement_tstamp = lambda x: pd.to_datetime(x.measurement_tstamp, format='%Y-%m-%d %H:%M:%S'),
                            date = lambda x: x.measurement_tstamp.dt.date)
                    .rename(columns = {'measurement_tstamp': 'Hour'}))
            #df.Hour = df.Hour.dt.tz_localize('America/New_York')
            df = df.drop_duplicates() # Shouldn't be needed anymore since we're using list(set(tmc_df.tmc.values))
            
            # -- Raw Hourly Travel Time Data --
            def uf(df):
                date_string = df.date.values[0]
                filename = 'travel_times_{}.parquet'.format(date_string)
                # df.drop(columns=['date']).to_parquet(filename)
                # s3.upload_file(Bucket = 'gdot-spm', 
                #                Filename = filename, 
                #                Key = 'mark/travel_times/date={}/{}'.format(date_string, filename))
                df.drop(columns=['date'])\
                    .to_parquet('s3://gdot-spm/mark/travel_times/date={}/{}'.format(date_string, filename))
                 
            # Write to parquet files and upload to S3
            df.groupby(['date']).apply(uf)
            
            
    
            # -- Travel Time Metrics Summarized by tti, pti by hour --
            df_ = df.groupby(['Corridor', 'Hour'], as_index=False)['travel_time_minutes', 'reference_minutes'].sum()
            #df_['Hour'] = df_['Hour'] - pd.to_timedelta(df_['Hour'].dt.day - 1, unit = 'days')
            df_['Hour'] = df_['Hour'].apply(lambda x: x.replace(day=1))
    
            desc = df_.groupby(['Corridor', 'Hour']).describe(percentiles = [0.90])
            tti = desc['travel_time_minutes']['mean'] / desc['reference_minutes']['mean']
            pti = desc['travel_time_minutes']['90%'] / desc['reference_minutes']['mean']
    
            summ_df = pd.DataFrame({'tti': tti, 'pti': pti})
    
            def uf(df): # upload parquet file
                date_string = df.date.values[0]
                filename = 'travel_time_metrics_{}.parquet'.format(date_string)
                df.drop(columns=['date'])\
                    .to_parquet('s3://gdot-spm/mark/travel_time_metrics/date={}/{}'.format(date_string, filename))
                    
            # Write to parquet files and upload to S3
            summ_df.reset_index().assign(date = lambda x: x.Hour.dt.date).groupby(['date']).apply(uf)
        else:
            print('No records returned.')
