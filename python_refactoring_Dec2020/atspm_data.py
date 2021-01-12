# -*- coding: utf-8 -*-
"""
Created on Wed Dec  9 20:11:36 2020

@author: Alan.Toppen
"""

import pandas as pd
import boto3


s3_resource = boto3.resource('s3')


def get_atspm_s3filenames(date, bucket):
    s3prefix = f'atspm/date={date}'
    objs = (s3_resource
            .Bucket(bucket)
            .objects
            .filter(Prefix=s3prefix))
    s3filenames = [f's3://{bucket}/{obj.key}' for obj in objs]
    return s3filenames


def get_atspm_data(s3filename):
    df = pd.read_parquet(s3filename)
    return df
    

def get_raw_counts(df, interval, eventcode):
    df = df[df.EventCode==eventcode].copy()
    df.Timestamp = df.Timestamp.dt.floor(interval)
    
    df = (df.drop(columns=['DeviceID', 'date'])
          .rename(columns={'EventParam': 'Detector'})
          .groupby(['SignalID', 'Timestamp', 'Detector'])
          .count()
          .reset_index()
          .rename(columns={'EventCode': 'Volume'}))
    
    return df


def get_raw_veh_counts(atspm_data, interval):
    return get_raw_counts(atspm_data, interval, eventcode=82)


def get_raw_ped_counts(atspm_data, interval):
    return get_raw_counts(atspm_data, interval, eventcode=90)

