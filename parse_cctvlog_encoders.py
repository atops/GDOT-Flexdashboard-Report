# -*- coding: utf-8 -*-
"""
Created on Mon Aug 12 14:04:15 2019

@author: Alan.Toppen
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
from multiprocessing import Pool
from multiprocessing import get_context

s3 = boto3.client('s3')
ath = boto3.client('athena', region_name='us-east-1')


def parse_cctvlog_encoders(key):
    key_date = re.search('\d{4}-\d{2}-\d{2}', key).group()
    try:
        df = (
            pd.read_parquet(
                's3://gdot-spm/{}'.format(key),
                columns=[
                    'Location ID', 'Datetime', 'Size'
                ]).rename(columns={
                    'Location ID': 'CameraID',
                    'Datetime': 'Timestamp',
                    'Size': 'Size'
                }).assign(Timestamp=lambda x: pd.to_datetime(
                    x.Timestamp, format='%Y-%m-%d %H:%M:%S'
                ).dt.tz_localize('UTC').dt.tz_convert(
                    'America/New_York').dt.tz_localize(None)).assign(
                        Date=pd.Timestamp(
                            key_date).date())
            .drop_duplicates())
    except:
        print('Problem reading {}'.format(key))
        df = pd.DataFrame()

    return df


if __name__ == '__main__':
    td = date.today()
    som = td - timedelta(td.day - 1)
    sopm = som - relativedelta(months=1)
    months = pd.date_range(start=sopm, periods=2, freq='MS')

    for mo in months:
        print(mo.strftime('%Y-%m-%d'))
        objs = s3.list_objects(Bucket='gdot-spm',
                               Prefix='mark/cctvlogs_encoder/date={}'.format(
                                   mo.strftime('%Y-%m')))

        if 'Contents' in objs:
            keys = [contents['Key'] for contents in objs['Contents']]
            keys = [
                key for key in keys
                if re.search(td.strftime('%Y-%m-%d'), key) == None
            ]
            print(len(keys))

            if len(keys) > 0:

                with get_context('spawn').Pool() as pool:
                    dfs = pool.map_async(parse_cctvlog_encoders, keys)
                    pool.close()
                    pool.join()

                df = pd.concat(dfs.get()).drop_duplicates()

                # Daily summary (stdev of image size for the day as a proxy for uptime: sd > 0 = working)
                summ = df.groupby(['CameraID', 'Date']).agg(np.std).fillna(0)

                s3key = 's3://gdot-spm/mark/cctv_uptime_encoders/month={d}/cctv_uptime_encoders_{d}.parquet'.format(
                    d=mo.strftime('%Y-%m-%d'))
                summ.reset_index().to_parquet(s3key)

        os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

        response_repair = ath.start_query_execution(
            QueryString='MSCK REPAIR TABLE cctv_uptime_encoders',
            QueryExecutionContext={'Database': 'gdot_spm'},
            ResultConfiguration={'OutputLocation': 's3://gdot-spm-athena'})

        while True:
            response = s3.list_objects(
                Bucket='gdot-spm-athena',
                Prefix=response_repair['QueryExecutionId'])
            if 'Contents' in response:
                break
            else:
                time.sleep(2)
