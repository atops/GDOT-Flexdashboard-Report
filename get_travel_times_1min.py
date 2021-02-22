# -*- coding: utf-8 -*-
"""
Created on Mon Sep 24 20:42:51 2018

@author: Alan.Toppen
"""


import os #AG update
import pandas as pd
import numpy as np
import requests
import uuid
import polling
import time
import yaml
from datetime import date, datetime, timedelta
import pytz
from zipfile import ZipFile
import json
import io
import boto3
import dask.dataframe as dd

#os.environ['TZ'] = 'America/New_York'
#time.tzset()

s3 = boto3.client('s3')

pd.options.display.max_columns = 10

# working directory to get the corridor and TMC spreadsheets - will need to update this - AG update
os.chdir('C:\\Users\\Anthony.Gallo\\OneDrive - KH\\ATSPM\\Health Metrics Dashboard\\GDOT-Flexdashboard-Report-master_2020-11-30')


def is_success(response):
    x = json.loads(response.content.decode('utf-8'))
    if 'state' in x.keys() and x['state']=='SUCCEEDED':
        return True
    else:
        # print(f"state: {x['state']} | progress: {x['progress']}")
        return False


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
      "dow": [ #only pull T-Th data
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
            "min": 0.71, #only accept data w/ quality >= 0.71
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
        "value": 1 #AG update
      },
      "times": [ #pulling all 24 hours
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
        jobid = json.loads(response.content.decode('utf-8'))['id']

        polling.poll(
            lambda: requests.get(uri.format('jobs/status'), params = {'key': key, 'jobId': jobid}),
            check_success = is_success,
            step=10,
            timeout=3600) #changed to allow for up to 1 hour to be safe

        results = requests.get(uri.format('jobs/export/results'), 
                               params = {'key': key, 'uuid': payload['uuid']})
        print('results received')
        
        # Save results (binary zip file with one csv)
        with io.BytesIO() as f:
            f.write(results.content)
            f.seek(0)
            #df = pd.read_csv(f, compression='zip')       
            with ZipFile(f, 'r') as zf:
                df = pd.read_csv(zf.open('Readings.csv'))
                #tmci = pd.read_csv(zf.open('TMC_Identification.csv'))

    else:
        df = pd.DataFrame()

    print('{} records'.format(len(df)))
    
    return df


def get_rsi(df, df_speed_limits, corridor_grouping): #relative speed index = 90th % speed / speed limit
    df_rsi = df.groupby(corridor_grouping).speed.quantile(0.90).reset_index()
    df_rsi = pd.merge(df_rsi, df_speed_limits[corridor_grouping + ['Speed Limit']])
    rsi = df_rsi['speed'] / df_rsi['Speed Limit']
    df_rsi['rsi'] = rsi
    return df_rsi


def clean_up_tt_df_for_bpsi(df):
    df = df[['tmc_code', 'Corridor', 'Subcorridor', 'Minute', 'speed']]
    df = df[df.speed >= 20]
    df = df.assign(hr = lambda x: x.Minute.dt.hour) #takes a while - put after filter
    df = df[df.hr.between(9,10) | df.hr.between(19,20)] #only take rows w/ speed > 20 and between 9-11 AM/7-9 PM
    return df


def get_serious_injury_pct(df, df_reference, corridor_grouping):
    #get count of each grouping for calculating percentages, merge back into overall df
    df_totals = df.groupby(corridor_grouping).size().reset_index(name='totals')
    df = pd.merge(df, df_totals)
    
    #get count for each speed bin by grouping, calculating the % out of the total for that grouping, and again merge back into overall df
    df_count = (df[corridor_grouping + ['speed','totals']].groupby(corridor_grouping + ['speed']).agg(['count','mean'])).reset_index()
    count_pct = df_count['totals']['count'] / df_count['totals']['mean']
    df_count['count_pct'] = count_pct
    df_summary = df_count[corridor_grouping + ['speed','count_pct']].droplevel(1, axis = 1)
    
    #merge w/ reference_df and calculate overall serious injury %
    df_summary = pd.merge(df_summary, df_reference, left_on=['speed'], right_on=['mph_bin'])
    overall_pct = df_summary['count_pct'] * df_summary['pct']
    df_summary['overall_pct'] = overall_pct
    df_summary = df_summary.groupby(corridor_grouping, as_index=False)['overall_pct'].sum()
    return df_summary


if __name__=='__main__':

    with open('Monthly_Report_AWS.yaml') as yaml_file: #AG - this was not in the latest GDOT-flexdashboard-report folder - had to copy in
        cred = yaml.load(yaml_file, Loader=yaml.Loader)

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    #start/end dates should be first and last days of previous month
    end_date = date.today().replace(day=1) - timedelta(days=1)
    start_date = end_date.replace(day=1)
    
    end_date = end_date.strftime('%Y-%m-%d')
    start_date = start_date.strftime('%Y-%m-%d')
    
#    #pull in file that matches up corridors/subcorridors/TMCs from S3 - having certificate issue on AG end
#    tmc_df = (pd.read_excel('s3://{b}/{f}'.format(
#                    b=conf['bucket'], f=conf['corridors_TMCs_filename_s3']))
#                .rename(columns={'length': 'miles'})
#                .fillna(value={'Corridor': 'None', 'Subcorridor': 'None'}))
    tmc_df = (pd.read_excel("Corridor_TMCs_Latest.xlsx") #AG update - don't have package to handle S3 files 
                .rename(columns={'length': 'miles'})
                .fillna(value={'Corridor': 'None', 'Subcorridor': 'None'}))
    tmc_df = tmc_df[tmc_df.Corridor != 'None'] #4500 rows
    #tmc_df = tmc_df[(tmc_df.Corridor == "Peachtree St-Midtown") | (tmc_df.Corridor == "Peachtree St-Downtown")] #test - 95 rows

    tmc_list = list(set(tmc_df.tmc.values))

    print(start_date)
    print(end_date)
    number_of_days = (datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")).days #AG add

    # group_size = 1000
    # tmc_groups = np.split(tmc_list, range(group_size, len(tmc_list), group_size))

    try:
        start_time = time.perf_counter()
        tt_df = get_tmc_data(start_date, end_date, tmc_list, cred['RITIS_KEY'], 0)
        # tt_df = pd.concat(
        #     [get_tmc_data(start_date, end_date, list(tmc_group), cred['RITIS_KEY'], 1) for tmc_group in tmc_groups]
        # )
        end_time = time.perf_counter()
        process_time = end_time - start_time
        print('Time to pull ', len(tmc_list), ' TMCs for ', number_of_days, ' days: ', round(process_time))

    except Exception as e:
        print('ERROR retrieving records')
        print(e)
        tt_df = pd.DataFrame()
    
    if len(tt_df) > 0:
        df = (pd.merge(tmc_df[['tmc', 'miles', 'Corridor', 'Subcorridor']], tt_df, left_on=['tmc'], right_on=['tmc_code'])
                .drop(columns=['tmc'])
                .sort_values(['Corridor', 'tmc_code', 'measurement_tstamp']))

        df['reference_minutes'] = df['miles'] / df['reference_speed'] * 60
        df = (df.reset_index(drop=True)
                .assign(measurement_tstamp = lambda x: pd.to_datetime(x.measurement_tstamp, format='%Y-%m-%d %H:%M:%S'),
                        date = lambda x: x.measurement_tstamp.dt.date)
                .rename(columns = {'measurement_tstamp': 'Minute'}))
                #.rename(columns = {'measurement_tstamp': 'Hour'}))
        #df.Hour = df.Hour.dt.tz_localize('America/New_York')
        df = df.drop_duplicates() # Shouldn't be needed anymore since we're using list(set(tmc_df.tmc.values))
        #df.to_csv('travel_times_1min_{}.csv'.format(date_string), sep = ',') #AG test
        
        #write 1-min monthly travel times/speed df to parquet on S3
        date_string = start_date
        bucket = conf['bucket']
        table_name = 'travel_times_1min'
        filename = 'travel_times_1min_{}.parquet'.format(date_string)
        df.drop(columns=['date'])\
            .to_parquet('s3://{b}/mark/{t}/{f}'.format(
                    b=bucket, t=table_name, f=filename))
        
        #############################################
        # relative speed index for month
        #############################################
        os.chdir('C:\\Users\\Anthony.Gallo\\OneDrive - KH\\ATSPM\\Health Metrics Dashboard') #update to read from somewhere on S3
        df_speed_limits = pd.read_excel("Corridors_Latest.xlsx", sheet_name="Contexts")

        df_rsi_sub = get_rsi(df, df_speed_limits, ['Corridor','Subcorridor'])
        df_rsi_cor = get_rsi(df, df_speed_limits[df_speed_limits['Subcorridor'].isnull()], ['Corridor'])

        #do we need to add a column to this that has month? right now is just grouping/RSI
        table_name = 'relative_speed_index'
        filename = 'rsi_sub_{}.parquet'.format(date_string)
        df_rsi_sub.to_parquet('s3://{b}/mark/{t}/{f}'.format(b=bucket, t=table_name, f=filename))
        filename = 'rsi_cor_{}.parquet'.format(date_string)
        df_rsi_cor.to_parquet('s3://{b}/mark/{t}/{f}'.format(b=bucket, t=table_name, f=filename))

        df_rsi_sub.to_csv('rsi_sub_{}.csv'.format(date_string), index=False)
        df_rsi_cor.to_csv('rsi_cor_{}.csv'.format(date_string), index=False)
        
        #############################################
        # bike-ped safety index for month
        #############################################
        os.chdir('C:\\Users\\Anthony.Gallo\\OneDrive - KH\\ATSPM\\Health Metrics Dashboard\\Ped Bike Safety Index') #update to read from somewhere on S3
        df_reference = pd.read_csv("serious_injury_pct.csv") #update to read from somewhere on S3
        
        df_bpsi = clean_up_tt_df_for_bpsi(df)
        
        df_bpsi_sub = get_serious_injury_pct(df_bpsi, df_reference, ['Corridor','Subcorridor'])
        df_bpsi_cor = get_serious_injury_pct(df_bpsi, df_reference, ['Corridor'])
        
        #do we need to add a column to this that has month? right now is just grouping/serious injury %
        table_name = 'bike_ped_safety_index'
        filename = 'bpsi_sub_{}.parquet'.format(date_string)
        df_bpsi_sub.to_parquet('s3://{b}/mark/{t}/{f}'.format(b=bucket, t=table_name, f=filename))
        filename = 'bpsi_cor_{}.parquet'.format(date_string)
        df_bpsi_cor.to_parquet('s3://{b}/mark/{t}/{f}'.format(b=bucket, t=table_name, f=filename))
        
        df_bpsi_sub.to_csv('bpsi_sub_{}.csv'.format(date_string), index=False)
        df_bpsi_cor.to_csv('bpsi_cor_{}.csv'.format(date_string), index=False)

    else:
        print('No records returned.')
