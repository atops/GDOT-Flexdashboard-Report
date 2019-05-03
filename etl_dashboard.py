# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 16:27:29 2017

@author: Alan.Toppen
"""
from datetime import datetime, timedelta
from multiprocessing.dummy import Pool
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
from parquet_lib import *

os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

s3 = boto3.client('s3')
ath = boto3.client('athena' )


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

def etl2(s, date_):
    
    date_str = date_.strftime('%Y-%m-%d')
    dc_fn = 'ATSPM_Det_Config_Good_{}.feather'.format(date_str)
    """
    if ~os.path.exists(dc_fn):
        s3.download_file(Filename = dc_fn, 
                         Bucket = 'gdot-devices', 
                         Key = 'atspm_det_config_good/date={}/ATSPM_Det_Config_Good.feather'.format(date_str))
    """
    det_config = (feather.read_dataframe(dc_fn)
                    .assign(SignalID = lambda x: x.SignalID.astype('int64'))
                    .assign(Detector = lambda x: x.Detector.astype('int64'))
                    .rename(columns={'CallPhase': 'Call Phase'}))
    
    left = det_config[det_config.SignalID==s]
    right = bad_detectors[(bad_detectors.SignalID==s) & (bad_detectors.Date==date_)]
        
    det_config_good = (pd.merge(left, right, how = 'outer', indicator = True)
                         .loc[lambda x: x._merge=='left_only']
                         .drop(['Date','_merge'], axis=1))

    start_date = date_
    end_date = date_ + pd.DateOffset(days=1) - pd.DateOffset(seconds=0.1)
    
    
    t0 = time.time()
    
    print('{} | {} Starting...'.format(s, date_str))

    #try:
    print('|{} reading from database...'.format(s))

    key = 'atspm/date={d}/atspm_{s}_{d}.parquet'.format(s = s, d = date_str)
    df = read_parquet_file('gdot-spm', key)
    
    #print("df", df.head(3))
    #print("det_config", det_config_good)
    
    if len(df)==0:
        print('|{} no event data for this signal on {}.'.format(s, date_str))

    if len(det_config_good)==0:
        print('|{} no detector configuration data for this signal on {}.'.format(s, date_str))
        
    if len(df) > 0 and len(det_config_good) > 0:

        print('|{} creating cycles and detection events...'.format(s))
        c, d = etl_main(df, det_config_good)

        if len(c) > 0 and len(d) > 0:

            # print('writing to files...')

            # if not os.path.exists('../CycleData/' + date_str):
            #     os.mkdir('../CycleData/' + date_str)
            # if not os.path.exists('../DetectionEvents/' + date_str):
            #     os.mkdir('../DetectionEvents/' + date_str)


            c.to_parquet('s3://gdot-spm-cycles/date={}/cd_{}_{}.parquet'.format(date_str, s, date_str), 
                         allow_truncated_timestamps=True)

            d.to_parquet('s3://gdot-spm-detections/date={}/de_{}_{}.parquet'.format(date_str, s, date_str), 
                         allow_truncated_timestamps=True)


            print('{}: {} seconds'.format(s, int(time.time()-t0)))
        else:
            print('{}: {} seconds -- no cycles'.format(s, int(time.time()-t0)))
        
    
    #except Exception as e:
    #    print(s, e)


        
        
    
if __name__=='__main__':

    

    
        
    
    
    #corridors = pd.read_feather("GDOT-Flexdashboard-Report/corridors.feather")
    #signalids = list(corridors.SignalID.astype('int').values)
    
    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file)

    start_date = conf['start_date']
    if start_date == 'yesterday': 
        start_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = conf['end_date']
    if end_date == 'yesterday': 
        end_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    #-----------------------------------------------------------------------------------------
    # Placeholder for manual override of start/end dates
    #start_date = '2019-02-03'
    #end_date = '2019-02-03'
    #-----------------------------------------------------------------------------------------
    
    dates = pd.date_range(start_date, end_date, freq='1D')
    print(dates)
                                       
    corridors_filename = conf['corridors_filename']
    corridors = feather.read_dataframe(corridors_filename)
    corridors = corridors[~corridors.SignalID.isna()]
    
    signalids = list(corridors.SignalID.astype('int').values)
    
    for date_ in dates:

        t0 = time.time()
        
        #query = "SELECT * FROM gdot_spm.bad_detectors WHERE date = '{}';"
        #       
        #with connect(s3_staging_dir='s3://gdot-spm-athena', region_name='us-east-1') as conn:
        #    bad_detectors = (pd.read_sql(sql = query.format(date_.strftime('%Y-%m-%d')), con=conn)
        #                       .rename(columns = {'date': 'Date', 
        #                                          'signalid': 'SignalID',
        #                                          'detector': 'Detector', 
        #                                          'good_day': 'Good_Day'}))

        date_str = date_.strftime('%Y-%m-%d')
        dc_fn = 'ATSPM_Det_Config_Good_{}.feather'.format(date_str)
        s3.download_file(Filename = dc_fn, 
                         Bucket = 'gdot-devices', 
                         Key = 'atspm_det_config_good/date={}/ATSPM_Det_Config_Good.feather'.format(date_str))
        
        key = 'mark/bad_detectors/date={d}/bad_detectors_{d}.parquet'.format(d = date_str)
        bad_detectors = read_parquet_file('gdot-spm', key)
        
        #key = 'mark/bad_detectors/date={d}/bad_detectors_{d}.parquet'.format(d = date_str)
        #bad_detectors = pd.read_parquet('s3://gdot-spm/{}'.format(key))
        
        ncores = os.cpu_count()

        #-----------------------------------------------------------------------------------------
        pool = Pool(ncores * 4) #24
        asyncres = pool.starmap(etl2, list(itertools.product(signalids, [date_])))
        pool.close()
        pool.join()
        #-----------------------------------------------------------------------------------------
        
        
        
        #for s in signalids:
        #etl2(7314, date_)
    
        #template_string = 'ALTER TABLE cycle_data add partition (date="{d}") location "s3://gdot-spm-cycles/"'
        #partition_query = template_string.format(d = date_str)
        #print(partition_query)
        
        #response = ath.start_query_execution(QueryString = partition_query, 
        #                                         QueryExecutionContext={'Database': 'gdot_spm'},
        #                                         ResultConfiguration={'OutputLocation': 's3://gdot-spm-athena'})
        os.remove(dc_fn)
        
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
        
    response_repair_cycledata = ath.start_query_execution(
                QueryString='MSCK REPAIR TABLE cycledata', 
                QueryExecutionContext={'Database': 'gdot_spm'},
                ResultConfiguration={'OutputLocation': 's3://gdot-spm-athena'})

    response_repair_detection_events = ath.start_query_execution(
                QueryString='MSCK REPAIR TABLE detectionevents', 
                QueryExecutionContext={'Database': 'gdot_spm'},
                ResultConfiguration={'OutputLocation': 's3://gdot-spm-athena'})
        
    print('\n{} signals in {} days. Done in {} minutes'.format(len(signalids), len([date_]), int((time.time()-t0)/60)))

        
    while True:
        response1 = s3.list_objects(Bucket='gdot-spm-athena', Prefix=response_repair_cycledata['QueryExecutionId'])
        response2 = s3.list_objects(Bucket='gdot-spm-athena', Prefix=response_repair_detection_events['QueryExecutionId'])
        if 'Contents' in response1 and 'Contents' in response2:
            print('done.')
            break
        else:
            time.sleep(2)
            print('.', end='')
