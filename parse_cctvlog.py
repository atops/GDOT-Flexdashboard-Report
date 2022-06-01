# -*- coding: utf-8 -*-
"""
Created on Tue Feb 20 11:10:19 2018

@author: V0010894
"""

import pandas as pd
import numpy as np
from datetime import datetime
from glob import glob
import yaml
import re
import os
import boto3
from datetime import date, timedelta
from dateutil.relativedelta import *
import time
import itertools
from multiprocessing import get_context

s3 = boto3.client('s3')
ath = boto3.client('athena', region_name='us-east-1')


def parse_cctvlog(bucket, key):
    key_date = re.search('\d{4}-\d{2}-\d{2}', key).group()
    try:
        df = (
            pd.read_parquet(
                f's3://{bucket}/{key}',
                columns=['ID', 'Last-Modified', 'Content-Length']).rename(
                    columns={
                        'ID': 'CameraID',
                        'Last-Modified': 'Timestamp',
                        'Content-Length': 'Size'
                    }).assign(
                        Timestamp=lambda x: pd.to_datetime(
                            x.Timestamp, format='%a, %d %b %Y %H:%M:%S %Z')
                        .dt.tz_convert('America/New_York').dt.tz_localize(None)
                    ).assign(Date=pd.Timestamp(
                        key_date).date())
            .drop_duplicates())
    except:
        print('Problem reading {}'.format(key))
        df = pd.DataFrame()

    return df


if __name__ == '__main__':

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)    
   
    bucket = conf['bucket']
 
    td = date.today()
    som = td - timedelta(td.day - 1)
    sopm = som - relativedelta(months=1)
    months = pd.date_range(start=sopm, periods=2, freq='MS')

    for mo in months:
        print(mo.strftime('%Y-%m-%d'))
        objs = s3.list_objects(Bucket=conf['bucket'],
                               Prefix='mark/cctvlogs/date={}'.format(
                                   mo.strftime('%Y-%m')))

        if 'Contents' in objs.keys():
            keys = [contents['Key'] for contents in objs['Contents']]
            keys = [
                key for key in keys
                if re.search(td.strftime('%Y-%m-%d'), key) == None
            ]
            print(len(keys))

            if len(keys) > 0:
                with get_context('spawn').Pool() as pool:
                    results = pool.starmap_async(parse_cctvlog, list(itertools.product([bucket], keys)))
                    pool.close()
                    pool.join()
                dfs = results.get()

                df = pd.concat(dfs).drop_duplicates()

                # Daily summary (stdev of image size for the day as a proxy for uptime: sd > 0 = working)
                summ = df.groupby(['CameraID', 'Date']).agg(np.std).fillna(0)

                s3key = 's3://{b}/mark/cctv_uptime/month={d}/cctv_uptime_{d}.parquet'.format(
                    b=bucket, d=mo.strftime('%Y-%m-%d'))
                summ.reset_index().to_parquet(s3key)

            os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

            response_repair = ath.start_query_execution(
                QueryString='MSCK REPAIR TABLE cctv_uptime',
                QueryExecutionContext={'Database': conf['bucket']},
                ResultConfiguration={'OutputLocation': conf['athena']['staging_dir']})

            while True:
                response = s3.list_objects(
                    Bucket=os.path.basename(conf['athena']['staging_dir']),
                    Prefix=response_repair['QueryExecutionId'])
                if 'Contents' in response:
                    break
                else:
                    time.sleep(2)
        else:
            print('No files to parse')
