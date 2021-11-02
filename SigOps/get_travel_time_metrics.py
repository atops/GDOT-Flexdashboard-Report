# -*- coding: utf-8 -*-
"""
Created on Mon Oct  4 14:43:57 2021

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




def get_corridor_travel_time_metrics(df, corr_grouping, bucket, table_name):
    
    df = df.groupby(corr_grouping + ['measurement_tstamp'], as_index=False)[
        ['travel_time_minutes', 'reference_minutes', 'miles']].sum()

    # -- Travel Time Metrics Summarized by tti, pti by timeperiod --
    df = df.rename(columns={'measurement_tstamp': 'Timeperiod'})
    df['Timeperiod'] = df['Timeperiod'].apply(lambda x: x.replace(day=1))
    df['speed'] = df['miles']/(df['travel_time_minutes']/60)

    desc = df.groupby(corr_grouping + ['Timeperiod']).describe(percentiles = [0.90])
    tti = desc['travel_time_minutes']['mean'] / desc['reference_minutes']['mean']
    pti = desc['travel_time_minutes']['90%'] / desc['reference_minutes']['mean']
    bi = pti - tti
    speed = desc['speed']['mean']

    summ_df = pd.DataFrame({'tti': tti, 'pti': pti, 'bi': bi, 'speed_mph': speed})

    def uf(df): # upload parquet file
        date_string = df.date.values[0]
        filename = f'travel_time_metrics_{date_string}.parquet'
        df.drop(columns=['date'])\
            .to_parquet(f's3://{bucket}/mark/interim/{table_name}/date={date_string}/{filename}')
            
    # Write to parquet files and upload to S3
    summ_df.reset_index().assign(date = lambda x: x.Timeperiod.dt.date).groupby(['date']).apply(uf)


if __name__=='__main__':

    with open(sys.argv[1]) as yaml_file:
        tt_conf = yaml.load(yaml_file, Loader=yaml.Loader)

    with open('Monthly_Report_AWS.yaml') as yaml_file:
        cred = yaml.load(yaml_file, Loader=yaml.Loader)

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    # start_date is either the given day or the first day of the month
    start_date = conf['start_date']
    if start_date == 'yesterday': 
        start_date = (datetime.now(pytz.timezone('America/New_York')) - timedelta(days=7))
    start_date = start_date.strftime('%Y-%m-%d')

    # end date is either the given date + 1 or today. 
    # end_date is not included in the query results
    end_date = conf['end_date']
    if end_date == 'yesterday': 
        end_date = datetime.now(pytz.timezone('America/New_York')) - timedelta(days=1)
    end_date = (end_date + timedelta(days=1)).strftime('%Y-%m-%d')


    suff = tt_conf['table_suffix']
    cor_table = f'cor_travel_times_{suff}'
    cor_metrics_table = f'cor_travel_time_metrics_{suff}'
    sub_table = f'sub_travel_times_{suff}'
    sub_metrics_table = f'sub_travel_time_metrics_{suff}'
    
   
    tmc_df = (pd.read_excel(f"s3://{conf['bucket']}/{conf['corridors_TMCs_filename_s3']}", 
                            engine='openpyxl')
                .rename(columns={'length': 'miles'})
                .fillna(value={'Corridor': 'None', 'Subcorridor': 'None'}))
    tmc_df = tmc_df[tmc_df.Corridor != 'None']
   
    for yyyy_mm in list(set([d.strftime('%Y-%m') for d in pd.date_range(start_date, end_date, freq='D')])):
        print(yyyy_mm)
        try:
            df = dd.read_parquet(f's3://gdot-spm/mark/travel_times_{suff}/date={yyyy_mm}-*/*').compute()
            if not df.empty:
                df = (pd.merge(tmc_df[['tmc', 'miles', 'Corridor', 'Subcorridor']], df, left_on=['tmc'], right_on=['tmc_code'])
                      .drop(columns=['tmc'])
                      .sort_values(['Corridor', 'tmc_code', 'measurement_tstamp']))
                df['reference_minutes'] = df['miles'] / df['reference_speed'] * 60

                get_corridor_travel_time_metrics(
                    df, ['Corridor'], conf['bucket'], cor_metrics_table)

                get_corridor_travel_time_metrics(
                    df, ['Corridor', 'Subcorridor'], conf['bucket'], sub_metrics_table)
        except IndexError:
            print(f'No data for {yyyy_mm}')

