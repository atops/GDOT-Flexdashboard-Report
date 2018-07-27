# -*- coding: utf-8 -*-
"""
Created on Sat Dec  2 13:27:20 2017

@author: Alan.Toppen
"""

import sys
import os
import pandas as pd
import numpy as np
import sqlalchemy as sq
#from multiprocessing.dummy import Pool
from multiprocessing import Pool
from pandas.tseries.offsets import MonthBegin, Day #, MonthEnd
import feather
from dask import delayed, compute

#engine = sq.create_engine('mssql+pyodbc://{}:{}@sqlodbc')
#conn_str = 'awsathena+jdbc://{access_key}:{secret_key}@athena.{region_name}.amazonaws.com:443/'\
#           '{schema_name}?s3_staging_dir={s3_staging_dir}'
#engine = sq.create_engine(conn_str.format(
#    access_key=os.environ['AWS_ACCESS_KEY_ID'],
#    secret_key=os.environ['AWS_SECRET_ACCESS_KEY'],
#    region_name='us-east-1',
#    schema_name='default',
#    s3_staging_dir='s3://gdot-spm-athena/'))

import boto3
from io import BytesIO
import polling

ath = boto3.client('athena')
s3 = boto3.client('s3')
s3r = boto3.resource('s3')

def query_athena(query, database, output_bucket):

    response = ath.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': 's3://{}'.format(output_bucket)
        }
    )
    print ('Started query.')
    # Wait for s3 object to be created
    polling.poll(
            lambda: 'Contents' in s3.list_objects(Bucket=output_bucket, 
                                                  Prefix=response['QueryExecutionId']),
            step=0.5,
            poll_forever=True)
    print ('Query complete.')
    key = '{}.csv'.format(response['QueryExecutionId'])
    s3.download_file(Bucket=output_bucket, Key=key, Filename=key)
    df = pd.read_csv(key)
    os.remove(key)
    #obj = s3.get_object(Bucket=output_bucket,
    #                    Key='{}.csv'.format(response['QueryExecutionId']))
    #df = pd.read_csv(BytesIO(obj['Body'].read()))
    print ('Results downloaded.')
    return df

## SPLIT FAILURES

def get_split_failures(start_date, end_date, signals_string):

    #with engine.connect() as conn:
    between_clause = "= '{}'".format(start_date.strftime('%Y-%m-%d'))
    
    cycle_query = """SELECT DISTINCT SignalID, Phase, PhaseStart 
                     FROM gdot_spm.CycleData 
                     WHERE Phase not in (2,6) 
                     AND EventCode = 9
                     AND date {}
                     AND SignalID in {}
                     """.format(between_clause, signals_string)
    
    detector_query = """SELECT DISTINCT SignalID, Phase, EventCode, DetTimeStamp as DetOn, DetDuration 
                        FROM gdot_spm.DetectionEvents 
                        WHERE Phase not in (2,6)
                        AND date {}
                        AND SignalID in {}
                        """.format(between_clause, signals_string)
                        
    print(between_clause)
    
    #sor = cd.query('Phase not in (2,6)')
    
    sor = (query_athena(cycle_query, 'gdot_spm', 'gdot-spm-athena')
            .assign(PhaseStart = lambda x: pd.to_datetime(x.PhaseStart)))

    #det = de.query('Phase not in (2,6)').rename(columns={'DetTimeStamp':'DetOn'})
    
    det = (query_athena(detector_query, 'gdot_spm', 'gdot-spm-athena')
            .assign(DetOn = lambda x: pd.to_datetime(x.DetOn))
            .assign(DetOff = lambda x: x.DetOn + pd.to_timedelta(x.DetDuration, unit='s')))

    sf = (pd.merge_asof(sor.sort_values(['PhaseStart']), 
                       det.sort_values(['DetOff']), 
                       by=['SignalID','Phase'], 
                       left_on=['PhaseStart'], 
                       right_on=['DetOff'], direction = 'forward')
            .assign(pre = lambda x: (x.DetOn - x.PhaseStart).astype('timedelta64[s]'))
            .assign(post = lambda x: (x.DetOff - x.PhaseStart).astype('timedelta64[s]'))
            .assign(split_failure = lambda x: (np.where((x.pre<0) & (x.post>10) & (x.post-x.pre<200), 1, 0)))
            .filter(items=['SignalID','Phase','PhaseStart','EventCode','pre','post','split_failure']))
    
    sf = (sf.assign(Hour = lambda x: x.PhaseStart.dt.floor('H'))
            .groupby(['SignalID','Phase','Hour'])['split_failure']
            .agg(['sum','count'])
            .rename(columns={'sum':'sf', 'count':'cycles'})
            .assign(sf_freq = lambda x: x.sf.astype('float')/x.cycles.astype('float'))
            #.filter(items=['sf_freq'])
            .reset_index())
    
    #sf.to_csv('C:/Users/alan.toppen/Code/GDOT/GDOT-Flexdashboard-Report/sf_{}.csv'.format(start_date.strftime('%Y-%m')))
    return sf
    
def helper(date_):
    start_date = date_
    end_date = start_date #+ pd.DateOffset(days=1) - pd.DateOffset(seconds=0.1)
    
    sf = get_split_failures(start_date, end_date, signals_string)
    
    # Moved into function since we're doing so many days. Reduce memory footprint
    sf = sf[sf.Hour != '2018-03-11 02:00:00']
    sf.Hour = sf.Hour.dt.tz_localize('US/Eastern', ambiguous=True)
    sf = sf.reset_index(drop=True)
    sf.to_feather('sf_{}.feather'.format(date_.strftime('%Y-%m-%d')))
        
if __name__=='__main__':

    #start_date = '2018-07-14'
    #end_date = '2018-07-22'

    
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    """
    signals_file = sys.argv[3]

    with open(signals_file) as f:
        signals = [s.strip() for s in f.readlines()]
    
    signals_to_exclude = [248,1389,3391,3491,
                          6329,6330,6331,6347,6350,6656,6657,
                          7063,7287,7289,7292,7293,7542,
                          71000,78296] + list(range(1600,1799))
    
    signals = list(set(signals) - set(signals_to_exclude))
    """
    corridors = pd.read_feather("GDOT-Flexdashboard-Report/corridors.feather")
    corridors = corridors[~corridors.SignalID.isna()]

    signals_list = list(corridors.SignalID.values)

    #signals_to_exclude = [248,1389,3391,3491,
    #                      6329,6330,6331,6347,6350,6656,6657,
    #                      7063,7287,7289,7292,7293,7542,
    #                      71000,78296] + list(range(1600,1799))    
    #signalids = list(set(signalids) - set(signals_to_exclude))
    
    signals_string = "({})".format(','.join(signals_list))
    
    dates = pd.date_range(start_date, end_date, freq=Day())
    

    
    results = []
    for d in dates:
        x = delayed(helper)(d)
        results.append(x)
    compute(*results)

