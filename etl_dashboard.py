# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 16:27:29 2017

@author: Alan.Toppen
"""
import sys
from datetime import datetime, timedelta
#from multiprocessing.dummy import Pool
from multiprocessing import get_context
import pandas as pd
import sqlalchemy as sq
import dask.dataframe as dd
#from pyathenajdbc import connect
#import pyodbc
import time
import os
import itertools
import boto3
import yaml
import io
import re
import psutil

from spm_events import etl_main
from parquet_lib import *

os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

s3 = boto3.client('s3')
ath = boto3.client('athena')

'''
    df:
        SignalID [int64]
        TimeStamp [datetime]
        EventCode [str or int64]
        EventParam [str or int64]
    
    det_config:
        SignalID [int64]
        IP [str]
        PrimaryName [str]
        SecondaryName [str]
        Detector [int64]
        Call Phase [int64]
'''

def etl2(s, date_, det_config):
    
    date_str = date_.strftime('%Y-%m-%d')

    det_config_good = det_config[det_config.SignalID==s]
    
    start_date = date_
    end_date = date_ + pd.DateOffset(days=1) - pd.DateOffset(seconds=0.1)
    
    
    t0 = time.time()
    

    try:
        key = f'atspm/date={date_str}/atspm_{s}_{date_str}.parquet'
        df = read_parquet_file('gdot-spm', key)
        
    
        if len(df)==0:
            print(f'{date_str} | {s} | No event data for this signal')

    
        if len(det_config_good)==0:
            print(f'{date_str} | {s} | No detector configuration data for this signal')
            
        if len(df) > 0 and len(det_config_good) > 0:
    
            c, d = etl_main(df, det_config_good)
    
            if len(c) > 0 and len(d) > 0:
    
                c.to_parquet(f's3://gdot-spm/cycles/date={date_str}/cd_{s}_{date_str}.parquet',
                             allow_truncated_timestamps=True)
    
                d.to_parquet(f's3://gdot-spm/detections/date={date_str}/de_{s}_{date_str}.parquet', 
                             allow_truncated_timestamps=True)
    
            else:
                print(f'{date_str} | {s} | No cycles')
        
    
    except Exception as e:
        print(f'{s}: {e}')


        
        
    


def main(start_date, end_date):
  
    
    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)
    
    #-----------------------------------------------------------------------------------------
    # Placeholder for manual override of start/end dates
    #start_date = '2019-06-04'
    #end_date = '2019-06-04'
    #-----------------------------------------------------------------------------------------
    
    dates = pd.date_range(start_date, end_date, freq='1D')

    corridors_filename = re.sub('\..*', '.feather', conf['corridors_filename_s3'])
    corridors = pd.read_feather(corridors_filename)
    corridors = corridors[~corridors.SignalID.isna()]
    
    signalids = list(corridors.SignalID.astype('int').values)
    
    
    #-----------------------------------------------------------------------------------------
    # Placeholder for manual override of signalids
    #signalids = [7053]
    #-----------------------------------------------------------------------------------------

    t0 = time.time()

    for date_ in dates:

        date_str = date_.strftime('%Y-%m-%d')

        print(date_str)

        with io.BytesIO() as data:
            s3.download_fileobj(
                Bucket='gdot-devices', 
                Key=f'atspm_det_config_good/date={date_str}/ATSPM_Det_Config_Good.feather',
                Fileobj=data)

            det_config_raw = pd.read_feather(data)\
                .assign(SignalID = lambda x: x.SignalID.astype('int64'))\
                .assign(Detector = lambda x: x.Detector.astype('int64'))\
                .rename(columns={'CallPhase': 'Call Phase'})

        try:
            bad_detectors = pd.read_parquet(
                f's3://gdot-spm/mark/bad_detectors/date={date_str}/bad_detectors_{date_str}.parquet')\
                        .assign(SignalID = lambda x: x.SignalID.astype('int64'))\
                        .assign(Detector = lambda x: x.Detector.astype('int64'))

            left = det_config_raw.set_index(['SignalID', 'Detector'])
            right = bad_detectors.set_index(['SignalID', 'Detector'])


            det_config = (left.join(right, how='left')
                .fillna(value={'Good_Day': 1})
                .query('Good_Day == 1')
                .reset_index(level='Detector')
                .set_index('Call Phase', append=True)
                .assign(
                    minCountPriority = lambda x: x.CountPriority.groupby(level=['SignalID', 'Call Phase']).min()))
            det_config['CountDetector'] = det_config['CountPriority'] == det_config['minCountPriority']
            det_config = det_config.drop(columns=['minCountPriority']).reset_index()

            print(det_config.head())

        except FileNotFoundError:
            det_config = pd.DataFrame()
        
        if len(det_config) > 0:    
            nthreads = round(psutil.virtual_memory().total/1e9)  # ensure 1 MB memory per thread

            #-----------------------------------------------------------------------------------------
            with get_context('spawn').Pool(processes=nthreads) as pool:
                result = pool.starmap_async(
                    etl2, list(itertools.product(signalids, [date_], [det_config])), chunksize=(nthreads-1)*4)
                pool.close()
                pool.join()
            #-----------------------------------------------------------------------------------------
        else:
            print('No good detectors. Skip this day.') 
        
    response_repair_cycledata = ath.start_query_execution(
                QueryString='MSCK REPAIR TABLE cycledata', 
                QueryExecutionContext={'Database': 'gdot_spm'},
                ResultConfiguration={'OutputLocation': 's3://gdot-spm-athena'})

    response_repair_detection_events = ath.start_query_execution(
                QueryString='MSCK REPAIR TABLE detectionevents', 
                QueryExecutionContext={'Database': 'gdot_spm'},
                ResultConfiguration={'OutputLocation': 's3://gdot-spm-athena'})
        
    print(f'{len(signalids)} signals in {len(dates)} days. Done in {int((time.time()-t0)/60)} minutes')

        
    while True:
        response1 = s3.list_objects(
            Bucket='gdot-spm-athena', 
            Prefix=response_repair_cycledata['QueryExecutionId'])
        response2 = s3.list_objects(
            Bucket='gdot-spm-athena', 
            Prefix=response_repair_detection_events['QueryExecutionId'])

        if 'Contents' in response1 and 'Contents' in response2:
            print('done.')
            break
        else:
            time.sleep(2)
            print('.', end='')


if __name__=='__main__':
    
    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)


    if len(sys.argv) > 1:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    else:
        start_date = conf['start_date']
        end_date = conf['end_date']
    
    if start_date == 'yesterday': 
        start_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    if end_date == 'yesterday': 
        end_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    main(start_date, end_date)

