# -*- coding: utf-8 -*-
"""
Created on Tue Feb 20 11:10:19 2018

@author: V0010894
"""
import pandas as pd
import numpy as np
from datetime import datetime
from glob import glob
import re
import os
import boto3
from datetime import date, timedelta
from dateutil.relativedelta import *
import time

s3 = boto3.client('s3')
ath = boto3.client('athena', region_name='us-east-1')

from dask import delayed, compute


def parse_cctvlog(key):

    df = (pd.read_parquet('s3://gdot-spm/{}'.format(key), columns=['ID', 'Last-Modified', 'Content-Length'])
            .rename(columns = {'ID': 'CameraID', 
                               'Last-Modified': 'Timestamp', 
                               'Content-Length': 'Size'})
            .assign(Timestamp = lambda x: pd.to_datetime(x.Timestamp, 
                                                         format ='%a, %d %b %Y %H:%M:%S %Z')
            #                                .dt.tz_localize('UTC')
                                            .dt.tz_convert('America/New_York')
                                            .dt.tz_localize(None))
            .assign(Date = lambda x: x.Timestamp.dt.date)
            .drop_duplicates())
    
    return df

td = date.today()
som = td - timedelta(td.day-1)
sopm = som - relativedelta(months=1)
months = pd.date_range(start=sopm, periods=2, freq = 'MS')

for mo in months:
    print(mo.strftime('%Y-%m-%d'))
    objs = s3.list_objects(Bucket = 'gdot-spm', Prefix = 'mark/cctvlogs/date={}'.format(mo.strftime('%Y-%m')))

    keys = [contents['Key'] for contents in objs['Contents']]
    keys = [key for key in keys if re.search(td.strftime('%Y-%m-%d'), key) == None]
    print(len(keys))
    
    if len(keys) > 0: 
        results = []
        for key in keys:
            x = delayed(parse_cctvlog)(key)
            results.append(x)
        dfs = compute(*results)
    
        df = pd.concat(dfs).drop_duplicates()

    
        # Daily summary (stdev of image size for the day as a proxy for uptime: sd > 0 = working)
        summ = df.groupby(['CameraID','Date']).agg(np.std).fillna(0)
    
        s3key = 's3://gdot-spm/mark/cctv_uptime/month={d}/cctv_uptime_{d}.parquet'.format(d=mo.strftime('%Y-%m-%d'))
        summ.reset_index().to_parquet(s3key)
        
        
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
        
    response_repair = ath.start_query_execution(
                QueryString='MSCK REPAIR TABLE cctv_uptime', 
                QueryExecutionContext={'Database': 'gdot_spm'},
                ResultConfiguration={'OutputLocation': 's3://gdot-spm-athena'})

        
    while True:
        response = s3.list_objects(Bucket='gdot-spm-athena', Prefix=response_repair['QueryExecutionId'])
        if 'Contents' in response:
            break
        else:
            time.sleep(2)
