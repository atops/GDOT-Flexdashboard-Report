# -*- coding: utf-8 -*-
"""
get_watchdog_alerts.py

Created on Thu Jul 26 14:36:14 2018

@author: V0010894
"""

import pandas as pd
import numpy as np
import sqlalchemy as sq
import pyodbc
import os
import io
import boto3
import zipfile
#import feather
import time
from datetime import datetime, timedelta

pd.options.display.max_columns = 10
s3 = boto3.client('s3')



import polling

ath = boto3.client('athena')
s3r = boto3.resource('s3')


def query_athena(query, database, output_bucket='gdot-spm-athena'):

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
            step=0.2,
            timeout=60)
    print ('Query complete.')
    key = '{}.csv'.format(response['QueryExecutionId'])
    time.sleep(1)
    s3.download_file(Bucket=output_bucket, Key=key, Filename=key)
    df = pd.read_csv(key)
    os.remove(key)

    print ('Results downloaded.')
    return df


def get_db_engine():

    if os.name == 'nt':
        
        uid = os.environ['ATSPM_USERNAME']
        pwd = os.environ['ATSPM_PASSWORD']
        dsn = 'atspm'
        connection_string = 'mssql+pyodbc://{}:{}@{}'.format(uid, pwd, dsn)
        
        engine = sq.create_engine(connection_string, pool_size=20)
    
    elif os.name == 'posix':
    
        def connect():
            return pyodbc.connect(
                'DRIVER=FreeTDS;' + 
                'SERVER={};'.format(os.environ["ATSPM_SERVER_INSTANCE"]) +
                'DATABASE={};'.format(os.environ["ATSPM_DB"]) +
                'UID={};'.format(os.environ['ATSPM_USERNAME']) +
                'PWD={};'.format(os.environ['ATSPM_PASSWORD']) +
                'TDS_Version=8.0;')
        
        engine = sq.create_engine('mssql://', creator=connect)
    
    return engine


# Read Corridors File from S3
def get_corridors():
    
    with io.BytesIO() as data:
        s3.download_fileobj(
                Bucket='gdot-spm', 
                Key='Corridors_Latest.feather', Fileobj=data)
        corridors = pd.read_feather(data)
    
    corridors = (corridors[~corridors.SignalID.isna()]
                .assign(SignalID = lambda x: x.SignalID.astype('int'))
                .drop(['Description'], axis=1))

    return corridors


# Upload watchdog alerts to predetermined location in S3
def s3_upload_watchdog_alerts(df):
    
    feather_filename = 'SPMWatchDogErrorEvents.feather'
    zipfile_filename = feather_filename + '.zip'
    
    df.to_feather(feather_filename)
    
    # Compress file
    zf = zipfile.ZipFile(zipfile_filename, 'w', zipfile.ZIP_DEFLATED)
    zf.write(feather_filename)
    zf.close()
    
    # Upload compressed file to s3
    s3.upload_file(Filename=zipfile_filename,
                   Bucket='gdot-spm', 
                   Key='mark/watchdog/{}'.format(zipfile_filename))
    os.remove(feather_filename)
    os.remove(zipfile_filename)


def get_watchdog_alerts(engine, corridors):

    # Query ATSPM Watchdog Alerts Table from ATSPM
    with engine.connect() as conn:
        SPMWatchDogErrorEvents = pd.read_sql_table('SPMWatchDogErrorEvents', con=conn)\
            .drop(columns=['ID'])\
            .drop_duplicates()
    
    # Join Watchdog Alerts with Corridors
    wd = SPMWatchDogErrorEvents.loc[SPMWatchDogErrorEvents.SignalID != 'null', ]
    wd = wd.loc[wd.TimeStamp > datetime.today() - timedelta(days=100)]
    wd = wd.fillna(value={'DetectorID': '0'})
    wd['Detector'] = np.vectorize(
            lambda a, b: a.replace(b, ''))(wd.DetectorID, wd.SignalID)
    wd = wd.drop(columns=['DetectorID'])
    wd.SignalID = wd.SignalID.astype('int')
    
    wd = (wd.set_index(['SignalID']).join(corridors.set_index(['SignalID']), how = 'left')
            .reset_index())
    wd = wd[~wd.Corridor.isna()]
    wd = wd.rename(columns = {'Phase': 'CallPhase',
                              'TimeStamp': 'Date'})
    
    # Clearn up the Message into a new field: Alert
    wd.loc[wd.Message.str.contains('Force Offs'), 'Alert'] = 'Force Offs'
    wd.loc[wd.Message.str.contains('Count'), 'Alert'] = 'Count'
    wd.loc[wd.Message.str.contains('Max Outs'), 'Alert'] = 'Max Outs'
    wd.loc[wd.Message.str.contains('Pedestrian Activations'), 'Alert'] = 'Pedestrian Activations'
    wd.loc[wd.Message.str.contains('Missing Records'), 'Alert'] = 'Missing Records'
    
    # Enforce Data Types
    wd.Alert = wd.Alert.astype('str')
    wd.Detector = wd.Detector.astype('int')
    wd.CallPhase = wd.CallPhase.astype('str')
    wd.CallPhase = wd.CallPhase.astype('category')
    wd.ErrorCode = wd.ErrorCode.astype('category')
    wd.Zone = wd.Zone.astype('category')
    wd.Zone_Group = wd.Zone_Group.astype('category')
    
    wd.Corridor = wd.Corridor.astype('category')
    wd.Name = wd.Name.astype('category')
    wd.Date = wd.Date.dt.date
    
    wd = wd.reset_index(drop=True)
    wd = wd.filter(['Zone_Group', 'Zone', 'Corridor', 
                    'SignalID', 'CallPhase', 'Detector', 
                    'Alert', 'Name', 'Date'], axis = 1)

    return wd
    #Zone_Group | Zone | Corridor | SignalID/CameraID | CallPhase | Detector | Date | Alert | Name


def main():
    
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:    
        engine = get_db_engine()
        corridors = get_corridors()
        
        wd = get_watchdog_alerts(engine, corridors)
        print('{now} - {alerts} watchdog alerts'.format(now=now, alerts=len(wd)))

        try:
            # Write to Feather file - WatchDog
            s3_upload_watchdog_alerts(wd)
            print('{now} - successfully uploaded to s3'.format(now=now))
        except Exception as e:
            print('{now} - ERROR: Could not upload to s3 - {err}'.format(now=now, err=e))

    except Exception as e:
        print('{now} - ERROR: Could not retrieve watchdog alerts - {err}'.format(now=now, err=e))
    

if __name__=='__main__':
    main()



