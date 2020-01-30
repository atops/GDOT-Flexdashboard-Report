# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 16:27:29 2017

@author: Alan.Toppen
"""
import sys
from datetime import datetime, timedelta
#from multiprocessing.dummy import Pool
from multiprocessing import Pool
import pandas as pd
import sqlalchemy as sq
from pyathenajdbc import connect
import pyodbc
import time
import os
import itertools
from spm_events import etl_main
import boto3
import yaml
import feather
import io
import re
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

def etl2(s, date_, det_config, conf):
    
    date_str = date_.strftime('%Y-%m-%d')

    det_config_good = det_config[det_config.SignalID==s]
    
    start_date = date_
    end_date = date_ + pd.DateOffset(days=1) - pd.DateOffset(seconds=0.1)
    
    
    t0 = time.time()
    
    print('{} | {} Starting...'.format(s, date_str))

    try:
        #print('|{} reading from database...'.format(s)) 
        key = 'atspm/date={d}/atspm_{s}_{d}.parquet'.format(s = s, d = date_str)
        df = read_parquet_file(conf['bucket'], key)
        
    
        if len(df)==0:
            print('|{} no event data for this signal on {}.'.format(s, date_str))
    
        if len(det_config_good)==0:
            print('|{} no detector configuration data for this signal on {}.'.format(s, date_str))
            
        if len(df) > 0 and len(det_config_good) > 0:
    
            #print('|{} creating cycles and detection events...'.format(s))
            c, d = etl_main(df, det_config_good)
    
            if len(c) > 0 and len(d) > 0:
    
                c.to_parquet(
                    's3://{b}/cycles/date={d}/cd_{s}_{d}.parquet'.format(
                        b=conf['bucket'], d=date_str, s=s), 
                    allow_truncated_timestamps=True)
    
                d.to_parquet(
                    's3://{b}/detections/date={d}/de_{s}_{d}.parquet'.format(
                        b=conf['bucket'], d=date_str, s=s), 
                    allow_truncated_timestamps=True)
    
    
                print('{}: {} seconds'.format(s, round(time.time()-t0, 1)))
            else:
                print('{}: {} seconds -- no cycles'.format(s, round(time.time()-t0, 1)))
        
    
    except Exception as e:
        print(s, e)


        
        
    


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
    corridors = feather.read_dataframe(corridors_filename)
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

        def f(bucket, key):
    
            with io.BytesIO() as data:
                s3.download_fileobj(
                    Bucket=bucket,
                    Key=key, 
                    Fileobj=data)

                det_config_raw = feather.read_dataframe(data)\
                    .assign(SignalID = lambda x: x.SignalID.astype('int64'))\
                    .assign(Detector = lambda x: x.Detector.astype('int64'))\
                    .rename(columns={'CallPhase': 'Call Phase'})
            return det_config_raw

        response = s3.list_objects_v2(
            Bucket = conf['bucket'], 
            Prefix = "atspm_det_config_good/date={}".format(date_str))
        keys = [res['Key'] for res in response['Contents']]

        det_config_raw = (pd.concat([f(conf['bucket'], key) for key in keys])
                .assign(SignalID = lambda x: x.SignalID.astype('int64'))
                .assign(Detector = lambda x: x.Detector.astype('int64'))
                .rename(columns={'CallPhase': 'Call Phase'}))

        try:
            bad_detectors = pd.read_parquet('s3://{b}/mark/bad_detectors/date={d}/bad_detectors_{d}.parquet'.format(
                            b=conf['bucket'], d=date_str))\
                    .assign(SignalID = lambda x: x.SignalID.astype('int64'))\
                    .assign(Detector = lambda x: x.Detector.astype('int64'))

            left = det_config_raw.set_index(['SignalID', 'Detector'])
            right = bad_detectors.set_index(['SignalID', 'Detector'])

            det_config = left.join(right, how='left')\
                .fillna(value={'Good_Day': 1})\
                .query('Good_Day == 1')\
                .groupby(['SignalID','Call Phase'])\
                .apply(lambda group: group.assign(CountDetector = group.CountPriority == group.CountPriority.min()))\
                .reset_index()

            print(det_config.head())

        except FileNotFoundError:
            det_config = pd.DataFrame()
        
        if len(det_config) > 0:    
            ncores = os.cpu_count()

            #-----------------------------------------------------------------------------------------
            with Pool(processes=ncores * 4) as pool: #24
                result = pool.starmap_async(
                    etl2, list(itertools.product(signalids, [date_], [det_config], [conf])), chunksize=(ncores-1)*4)
                pool.close()
                pool.join()
            #-----------------------------------------------------------------------------------------
        else:
            print('No good detectors. Skip this day.') 
        
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
        
    response_repair_cycledata = ath.start_query_execution(
                QueryString='MSCK REPAIR TABLE cycledata', 
                QueryExecutionContext={'Database': conf['athena']['database']},
                ResultConfiguration={'OutputLocation': conf['athena']['staging_dir']})

    response_repair_detection_events = ath.start_query_execution(
                QueryString='MSCK REPAIR TABLE detectionevents', 
                QueryExecutionContext={'Database': conf['athena']['database']},
                ResultConfiguration={'OutputLocation': conf['athena']['staging_dir']})
        
    print('\n{} signals in {} days. Done in {} minutes'.format(len(signalids), len(dates), int((time.time()-t0)/60)))

        
    while True:
        response1 = s3.list_objects(
            Bucket=os.path.basename(conf['athena']['staging_dir']), 
            Prefix=response_repair_cycledata['QueryExecutionId'])
        response2 = s3.list_objects(
            Bucket=os.path.basename(conf['athena']['staging_dir']), 
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

