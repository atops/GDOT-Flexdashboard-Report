# -*- coding: utf-8 -*-
"""
Created on Thu May 10 13:48:33 2018

@author: V0010894
"""
import os
import re
from multiprocessing.dummy import Pool
import pandas as pd
import sqlalchemy as sq
from glob import glob
from datetime import datetime, timedelta

import dask.dataframe as dd
from dask import delayed, compute
import boto3


def get_last_monday(time_stamp):
    return time_stamp - pd.Timedelta(days=time_stamp.weekday())


def get_table_name(table_prefix, time_stamp):
    mon = get_last_monday(time_stamp)
    return '{}_{}'.format(table_prefix, mon.strftime('%m-%d-%Y'))


def get_maxview_intersections(engine_maxv):

    ge = (pd.read_sql_table('GroupableElements', engine_maxv)
          .rename(columns={'ID': 'DeviceId',
                           'Number': 'SignalID'})
          .set_index(['DeviceId']))

    #gei = pd.read_sql_table('GroupableElements_IntersectionController', engine_maxv)

    ged = (pd.read_sql_table('GroupableElements_Device', engine_maxv)
           .rename(columns={'ID': 'DeviceId',
                            'DeviceConnectionPropertiesID': 'DcpId'})
           .dropna(subset=['DcpId'])
           .assign(DcpId=lambda x: x.DcpId.astype('int'))
           .set_index(['DcpId']))

    #ge = ge[ge.index.isin(gei.ID)]

    dcp = (pd.read_sql_table('DeviceConnectionProperties_IpDeviceSettings', engine_maxv)
           .rename(columns={'ID': 'DcpId'})
           .set_index(['DcpId']))

    dcp = dcp.join(ged.DeviceId, how='inner').set_index(['DeviceId'])

    maxt = (ge.join(dcp, how='left', lsuffix='_ge', rsuffix='_cp', sort=True)
            .dropna(subset=['HostAddress'])
            .reset_index()
            .set_index(['SignalID']))

    return maxt



def get_atspm_intersections(engine_atspm):

    sig = (pd.read_sql_table('Signals', engine_atspm)
           .query("SignalID != 'null'")
           .assign(SignalID=lambda x: x.SignalID.astype('int'))
           .assign(IPAddress=lambda x: x.IPAddress.apply(lambda y: re.sub('.*?(\d+\.\d+\.\d+\.\d+).*', '\\1', y)))
           .set_index(['SignalID'])
           .sort_index())

    return sig


def get_intersections_list(engine_maxv, engine_atspm, date_):

    #s3 = boto3.client('s3')

    sig = get_atspm_intersections(engine_atspm)
    maxt = get_maxview_intersections(engine_maxv)

    ints = sig.join(maxt, how='outer', lsuffix='_atspm', rsuffix='_maxv', sort=True)

    date_string = date_.strftime('%Y-%m-%d')
    #csv_filename = '{}_maxview_intersections.csv'.format(date_string)
    ints.reset_index().to_csv(
            's3://gdot-devices/maxv_atspm_intersections/date={}/maxv_atspm_intersections.csv'.format(date_string))
    #s3.upload_file(Filename=csv_filename,
    #               Bucket='gdot-devices',
    #               Key='maxv_atspm_intersections/date={}/maxv_atspm_intersections.csv'.format(date_string))
    #os.remove(csv_filename)
    #key = 'maxv_atspm_intersections/date={}/maxv_atspm_intersections.csv'.format(date_string)
    #ints.reset_index().to_csv('s3://gdot-devices/{}'.format(key))
    ints = ints.loc[~ints.DeviceId.isna()]
    ints.loc[:, 'DeviceId'] = ints.loc[:, 'DeviceId'].astype('int')
    ints = ints.reset_index().set_index(['DeviceId'])

    return ints


def csv_to_parquet(date_):

    date_string = date_.date().strftime('%Y-%m-%d')
    csv_filename = 'combined_events/{}/*.csv'.format(date_string)


    def f(fn):

        parquet_filename = fn.replace('.csv', '.parquet')
        signalid = re.sub('.*?(\d+)\.csv', '\\1', fn)

        df = pd.read_csv(fn)

        df_ = (df[['SignalID', 'Timestamp', 'EventCode', 'EventParam', '_merge']]
            .assign(SignalID=lambda x: x.SignalID.astype('uint16'))
            .assign(Timestamp=lambda x: pd.to_datetime(x.Timestamp))
            .assign(EventCode=lambda x: x.EventCode.astype('uint16'))
            .assign(EventParam=lambda x: x.EventParam.astype('int16'))
            .assign(_merge=lambda x: x._merge.astype('category')))

        df_.to_parquet(parquet_filename)
        s3.upload_file(Filename=parquet_filename,
                       Bucket='gdot-spm-events',
                       Key='date={}/{}.parquet'.format(date_string, signalid))
        os.remove(parquet_filename)
        print('.', end='')


        (df_.groupby(['SignalID','_merge'])
            .count()['Timestamp']
            .to_csv(raw_filename, header=False, mode='a'))

    raw_filename = 'combined_events/{}_raw.csv'.format(date_string)


    results = []
    for fn in glob(csv_filename):
        x = delayed(f)(fn)
        results.append(x)
    compute(*results)

    print('.')

    # aggregate by df, table, both, unstack and write to file
    (pd.read_csv(raw_filename, header=None, names=['SignalID', '_merge', 'Timestamp'])
       .set_index(['SignalID','_merge'])['Timestamp']
       .unstack(fill_value=0)
       .reset_index()
       .to_csv('combined_events/{}.csv'.format(date_string)))
    os.remove(raw_filename)



if __name__=="__main__":

    engine_maxv = sq.create_engine('mssql+pyodbc://{}:{}@MaxView'.format(os.environ['MAXV_USERNAME'], os.environ['MAXV_PASSWORD']), pool_size=20)
    engine_mvel = sq.create_engine('mssql+pyodbc://{}:{}@MaxView_EventLog'.format(os.environ['MAXV_USERNAME'], os.environ['MAXV_PASSWORD']), pool_size=20)
    engine_atspm = sq.create_engine('mssql+pyodbc://{}:{}@sqlodbc'.format(os.environ['ATSPM_USERNAME'], os.environ['ATSPM_PASSWORD']), pool_size=20)

    s3 = boto3.client('s3')

    yesterday = pd.Timestamp.today().date() - pd.Timedelta(1, unit='D')

    start_date = yesterday
    end_date = start_date
    dates = pd.DatetimeIndex(start=start_date, end=end_date, freq='D')


    for date_ in dates: # if d.weekday() in (1,2,3)]: # 0 - Monday, 1- Tuesday

        ints = get_intersections_list(engine_maxv, engine_atspm, date_)

        mon = get_last_monday(date_).date().strftime('%m-%d-%Y')
        asc_tables = [t for t in engine_mvel.table_names() if t.endswith(mon)]

        for table in asc_tables:
            print(table)