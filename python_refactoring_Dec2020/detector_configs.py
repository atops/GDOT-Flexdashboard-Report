# -*- coding: utf-8 -*-
"""
Created on Thu Dec 10 10:10:51 2020

@author: Alan.Toppen
"""

import pandas as pd
import numpy as np
import boto3
import re

s3_resource = boto3.resource('s3')

re_date = re.compile('(?<=date=)(\d{4}-\d{2}-\d{2})')
re_presence = re.compile('Stop Bar Presence')

det_config_dtypes = {} # Not needed since this is a feather file

ped_config_dtypes = {
    'SignalID': np.int64,
    'IP': object,
    'PrimaryName': object,
    'SecondaryName': object,
    'Detector': np.int64,
    'CallPhase': np.int64,
    'Date': object}



def get_detector_config_s3filenames(date, bucket, prefix):
    '''
    Get detector configuration filenames for a given date.

    Parameters
    ----------
    date : string
        Date.
    bucket : string
        bucket where configuration data lives on S3.
    prefix : string
        path one level below bucket for config data, e.g.,
        atspm_det_config_good - for vehicle detectors,
        maxtime_ped_plans - for pedestrian push buttons

    Returns
    -------
    list
        list of S3 file names.

    '''
    s3prefix = f'{prefix}/date={date}'
    objs = (s3_resource
            .Bucket(bucket)
            .objects
            .filter(Prefix=s3prefix))
    return [f's3://{bucket}/{obj.key}' for obj in objs]



def get_date_from_s3path(s3path):
    '''
    Get date for s3 data from structured file name.
    '''
    match = re_date.search(s3path)
    return match.group()



def get_detector_config(s3filenames):
    '''
    All Detector configuration data
    '''
    date = get_date_from_s3path(s3filenames[0])
    dfs = [pd.read_feather(f).assign(Date = date) for f in s3filenames]
    return pd.concat(dfs)



def get_ped_config(s3filenames):
    '''
    Ped plan data from maxtime
    '''
    date = get_date_from_s3path(s3filenames[0])
    dfs = [pd.read_csv(f, dtype=ped_config_dtypes).assign(Date = date) for f in s3filenames]
    df = pd.concat(dfs)
    df = df[['SignalID', 'IP', 
             'PrimaryName', 'SecondaryName', 
             'Detector', 'CallPhase', 
             'Date']]
    return df



def get_detector_config_vol(s3filenames):
    '''
    Detectors with the highest CountPriority in each CallPhase
    (lowest CountPriority value)

    '''
    dc = get_detector_config(s3filenames)
    dc = dc[['SignalID', 'Detector', 'CallPhase', 'CountPriority', 'TimeFromStopBar', 'Date']]
    dc.loc[:,'CountPriority'] = dc.loc[:,'CountPriority'].astype('int')
    dc.CountPriority = dc.CountPriority.astype('int')
    dc = dc.loc[dc.groupby(['SignalID', 'CallPhase', 'CountPriority'])['CountPriority'].idxmin()]
    return dc    



def get_detector_config_sf(s3filenames):
    '''
    Get the detector configuration data for use in the split failures calculation.
    Defined as detectors of type 'Stop Bar Presence.'
    '''
    dc = get_detector_config(s3filenames)
    dc = dc[['SignalID', 'Detector', 'CallPhase', 'TimeFromStopBar', 'DetectionTypeDesc', 'Date']]
    dc = dc[dc.DetectionTypeDesc.fillna('').str.contains('Stop Bar Presence')]
    return dc