# -*- coding: utf-8 -*-
"""
Created on Sat Feb 15 15:15:31 2020

@author: Alan.Toppen
"""

import os
import yaml
import time
import sys
from datetime import datetime, timedelta
import boto3
import pandas as pd
import io
import re
from multiprocessing import get_context
import itertools


def get_signalids(date_):

    bucket = 'gdot-spm'
    prefix = 'detections/date={d}'.format(d=date_.strftime('%Y-%m-%d'))

    objs = boto3.resource('s3').Bucket(bucket).objects.filter(Prefix=prefix)
    for page in objs.pages():
        for x in page:
            try:
                signalid = re.search('(?<=_)\d+(?=_)', x.key).group()
                yield signalid
            except:
                pass


def get_det_config(date_):
    '''
    date_ [Timestamp]
    '''
    date_str = date_.strftime('%Y-%m-%d')
    
    bd_key = 's3://gdot-spm/mark/bad_detectors/date={d}/bad_detectors_{d}.parquet'
    bd = pd.read_parquet(bd_key.format(d=date_str)).assign(
            SignalID = lambda x: x.SignalID.astype('int64'),
            Detector = lambda x: x.Detector.astype('int64'))

    dc_key = 'atspm_det_config_good/date={d}/ATSPM_Det_Config_Good.feather'
    with io.BytesIO() as data:
        boto3.resource('s3').Bucket('gdot-devices').download_fileobj(
                Key=dc_key.format(d=date_str), Fileobj=data)
        dc = pd.read_feather(data)[['SignalID', 'Detector', 'DetectionTypeDesc']]
    dc.loc[dc.DetectionTypeDesc.isna(), 'DetectionTypeDesc'] = '[]'

    df = pd.merge(dc, bd, how='outer', on=['SignalID','Detector']).fillna(value={'Good_Day': 1})
    df = df.loc[df.Good_Day==1].drop(columns=['Good_Day'])

    return df


def get_aog(signalid, date_, det_config, per):
    '''
    date_ [Timestamp]
    '''
    try:
        date_str = date_.strftime('%Y-%m-%d')
        
        all_hours = pd.date_range(date_, periods=25, freq=per)


        de_fn = f'../detections/Date={date_str}/SignalID={signalid}/de_{signalid}_{date_str}.parquet'
        if os.path.exists(de_fn):
            detection_events = pd.read_parquet(de_fn).drop_duplicates()
        else:
            de_fn = f's3://gdot-spm/detections/date={date_str}/de_{signalid}_{date_str}.parquet'
            detection_events = pd.read_parquet(de_fn).drop_duplicates()


        df = (pd.merge(
                detection_events,
                det_config[det_config.DetectionTypeDesc.str.contains('Advanced Count')],
                on=['SignalID', 'Detector'],
                how='left'))
        df = df[~df.DetectionTypeDesc.isna()]

        if df.empty:
            #print(f'{signalid}|{date_str}: No detectors for Arrivals on Green')
            print('#', end='')
            return pd.DataFrame()
        else:
            df_aog = (df.assign(Hour=lambda x: x.DetTimeStamp.dt.floor(per))
                      .rename(columns={'Detector': 'Arrivals',
                                       'EventCode': 'Interval'})
                      .groupby(['Hour', 'SignalID', 'Phase', 'Interval'])
                      .count()[['Arrivals']])
            df_aog['All_Arrivals'] = df_aog.groupby(level=[0, 1, 2]).transform('sum')
            df_aog['AOG'] = df_aog['Arrivals']/df_aog['All_Arrivals']

            aog = (df_aog.reset_index('Interval')
                   .query('Interval == 1')
                   .drop(columns=['Interval'])
                   .rename(columns={'Arrivals': 'Green_Arrivals'}))

            df_gc = (df[['SignalID', 'Phase', 'PhaseStart', 'EventCode']]
                     .drop_duplicates()
                     .rename(columns={'PhaseStart': 'IntervalStart',
                                      'EventCode': 'Interval'})
                     .assign(IntervalDuration=0)
                     .set_index(['SignalID', 'Phase', 'IntervalStart']))

            x = pd.DataFrame(
                    data={'Interval': None, 'IntervalDuration': 0},
                    index=pd.MultiIndex.from_product(
                            [df_gc.index.levels[0],
                             df_gc.index.levels[1],
                             all_hours],
                            names=['SignalID', 'Phase', 'IntervalStart']))

            df_gc = (pd.concat([df_gc, x])
                     .sort_index()
                     .ffill() # fill forward missing Intervals for on the hour rows
                     .reset_index(level=['IntervalStart'])
                     .assign(IntervalEnd=lambda x: x.IntervalStart.shift(-1))
                     .assign(IntervalDuration=lambda x: (x.IntervalEnd - x.IntervalStart).dt.total_seconds())
                     .assign(Hour=lambda x: x.IntervalStart.dt.floor(per))
                     .groupby(['Hour', 'SignalID', 'Phase', 'Interval']).sum())

            df_gc['Duration'] = df_gc.groupby(level=[0, 1, 2]).transform('sum')
            df_gc['gC'] = df_gc['IntervalDuration']/df_gc['Duration']
            gC = (df_gc.reset_index('Interval')
                  .query('Interval == 1')
                  .drop(columns=['Interval'])
                  .rename(columns={'IntervalDuration': 'Green_Duration'}))

            aog = pd.concat([aog, gC], axis=1).assign(pr=lambda x: x.AOG/x.gC)

            print('.', end='')

            return aog

    except Exception as e:
        print('{s}|{d}: Error--{e}'.format(e=e, s=signalid, d=date_str))
        return pd.DataFrame()


def main(start_date, end_date):

    dates = pd.date_range(start_date, end_date, freq='1D')

    for date_ in dates:
      try:

        t0 = time.time()
        print(date_)
    
        print('Getting detector configuration...', end='')
        det_config = get_det_config(date_)
        print('done.')
        print('Getting signals...', end='')
        signalids = get_signalids(date_)
        print('done.')

        print('1 hour')
        with get_context('spawn').Pool(24) as pool:
            results = pool.starmap_async(
                get_aog,
                list(itertools.product(signalids, [date_], [det_config], ['H'])))
            pool.close()
            pool.join()
    
        dfs = results.get()
        df = (pd.concat(dfs)
              .reset_index()[['SignalID', 'Phase', 'Hour', 'AOG', 'pr', 'All_Arrivals']]
              .rename(columns={'Phase': 'CallPhase',
                               'Hour': 'Date_Hour',
                               'AOG': 'aog',
                               'All_Arrivals': 'vol'})
              .sort_values(['SignalID', 'Date_Hour', 'CallPhase'])
              .fillna(value={'vol': 0})
              .assign(SignalID=lambda x: x.SignalID.astype('str'),
                      CallPhase=lambda x: x.CallPhase.astype('str'),
                      vol=lambda x: x.vol.astype('int32')))
    
        df.to_parquet('s3://gdot-spm/mark/arrivals_on_green/date={d}/aog_{d}.parquet'.format(
            d=date_.strftime('%Y-%m-%d')))
        num_signals = len(list(set(df.SignalID.values)))
        t1 = round(time.time() - t0, 1)
        print(f'\n{num_signals} signals done in {t1} seconds.')


        print('Getting signals...', end='')
        signalids = get_signalids(date_)
        print('done.')
        print('\n15 minutes')
        
        with get_context('spawn').Pool(24) as pool:
            results = pool.starmap_async(
                get_aog,
                list(itertools.product(signalids, [date_], [det_config], ['15min'])))
            pool.close()
            pool.join()
    
        dfs = results.get()
        df = (pd.concat(dfs)
              .reset_index()[['SignalID', 'Phase', 'Hour', 'AOG', 'pr', 'All_Arrivals']]
              .rename(columns={'Phase': 'CallPhase',
                               'Hour': 'Date_Period',
                               'AOG': 'aog',
                               'All_Arrivals': 'vol'})
              .sort_values(['SignalID', 'Date_Period', 'CallPhase'])
              .fillna(value={'vol': 0})
              .assign(SignalID=lambda x: x.SignalID.astype('str'),
                      CallPhase=lambda x: x.CallPhase.astype('str'),
                      vol=lambda x: x.vol.astype('int32')))
    
        df.to_parquet('s3://gdot-spm/mark/arrivals_on_green_15min/date={d}/aog_{d}.parquet'.format(
            d=date_.strftime('%Y-%m-%d')))
        num_signals = len(list(set(df.SignalID.values)))
        t1 = round(time.time() - t0, 1)
        print(f'\n{num_signals} signals done in {t1} seconds.')


      except Exception as e:
        print(f'{date_}: Error: {e}')



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

