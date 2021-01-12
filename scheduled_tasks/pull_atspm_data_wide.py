# -*- coding: utf-8 -*-
"""
Created on Mon Dec 23 16:32:22 2019

@author: Alan.Toppen
"""

import pandas as pd
import numpy as np
import sqlalchemy as sq
import io
#import feather
import boto3
from multiprocessing import Pool
import itertools
import re
import os
import sys
from datetime import datetime, timedelta

s3 = boto3.client('s3')
events_bucket = 'gdot-spm'
config_bucket = 'gdot-devices'


def read_atspm_query(query):
    engine = sq.create_engine('mssql+pyodbc://{}:{}@MaxView_EventLog'.format(
                    os.environ['MAXV_USERNAME'], 
                    os.environ['MAXV_PASSWORD']), 
                pool_size=20)
    
    with engine.connect() as con:
        df = pd.read_sql_query(query, con=con)
    return df


def get_eventlog_data_db(signalid, date_str):
    
    start_date = date_str
    end_date = (pd.Timestamp(date_str) + pd.DateOffset(days=1) - pd.DateOffset(seconds=0.1))\
                .strftime('%Y-%m-%d %H:%M:%S.%f')[:-5]
    
    df = read_atspm_query("""
            SELECT * FROM Controller_Event_Log 
            WHERE SignalID = '{}' 
			AND Timestamp BETWEEN '{}' AND '{}'
            ORDER BY SignalID, Timestamp, EventCode, EventParam
            """.format(signalid,
                        start_date,
                        end_date))
    return df


def get_eventlog_data(bucket, signalid, date_str):
    df = pd.read_parquet('s3://{b}/atspm/date={d}/atspm_{s}_{d}.parquet'.format(
            b=bucket, d=date_str, s=signalid))
    df.Timestamp = df.Timestamp.dt.tz_localize(None)
    df.SignalID = df.SignalID.astype('str')
    return df


def get_det_config(bucket, date_str):
    with io.BytesIO() as data:
        s3.download_fileobj(
                Bucket=bucket, 
                Key='atspm_det_config_good/date={}/ATSPM_Det_Config_Good.feather'.format(date_str), Fileobj=data)
    
        dc = pd.read_feather(data)\
            .assign(SignalID = lambda x: x.SignalID.astype('str'))\
            .assign(Detector = lambda x: x.Detector.astype('int64'))\
            .reset_index(drop=True)
    return dc
            #.rename(columns={'CallPhase': 'Call Phase'})\


def get_det_config_local(filename):
    
    dc = pd.read_feather(filename)\
        .assign(SignalID = lambda x: x.SignalID.astype('str'))\
        .assign(Detector = lambda x: x.Detector.astype('int64'))\
        .reset_index(drop=True)
        #.rename(columns={'CallPhase': 'Call Phase'})\
    return dc
    

def get_det_config_future(bucket, date_str):
    key = 's3://{b}/atspm_det_config_good/date={d}/ATSPM_Det_Config_Good.parquet'.format(
            b=bucket, d=date_str)
    print(key)
    dc = pd.read_parquet(key).reset_index(drop=True)
    return dc


# Works. Doesn't copy down. Use this for new grouping variable and copy_down to apply value across and down
def create_new_grouping_field(df, eventcodes, grouping_field, new_grouping_field, transform_func = lambda x: x):
    
    if type(eventcodes) is list:
        df.loc[df.EventCode.isin(eventcodes), new_grouping_field] = df.loc[df.EventCode.isin(eventcodes), grouping_field].apply(transform_func)
    else:
        eventcode = eventcodes
        df.loc[df.EventCode==eventcode, new_grouping_field] = df.loc[df.EventCode==eventcode, grouping_field].apply(transform_func)
    return df


# Works. Two-step create new field and copy down. May need just a copy down.
def copy_updown(
        df, eventcodes, new_field_name, group_fields, copy_field, 
        off_eventcode=None, direction='down', apply_to_timestamp='all'):
    '''
    df - eventlog dataframe
    eventcodes - EventCode(s) signifying event(s) to carry forward to subsequent events, e.g., 0 for PhaseStart
    new_field_name - name of Event corresponding to EventCode, e.g., PhaseStart
    group_fields - grouping(s) to which eventcode applies, e.g., [SignalID, EventParam] (Phase) for PhaseStart
    copy_field - field identifying eventcode, e.g., Timestamp for PhaseStart
    off_eventcode - optional value for where to stop copying up or down, otherwise goes to next value in eventcodes
    direction - 'up' for copy up, 'down' for copy down new_field_name
    apply_to_timestamp - 'all' to fill all rows with timestamps of the eventcodes before copying up or down,
                           Example would be 31-Barrier which should renew with all events at that same timestamp, 
                           of which there are many starts and ends to phase intervals
                         'group' to fill all rows at the timestamp in the group
                           Example would be Recorded Split
                         None to not fill all rows at the timestamp. 
                           Example detector off (81) or call off (44) events
    '''
    if type(eventcodes) is list:
        if sum(df.EventCode.isin(eventcodes)) == 0:
            #print('Event Codes {} not in data frame'.format(','.join(map(str, eventcodes))))
            return df
        else:
            df.loc[df.EventCode.isin(eventcodes), new_field_name] = df.loc[df.EventCode.isin(eventcodes), copy_field]
    else:
        eventcode = eventcodes
        if sum(df.EventCode==eventcode) == 0:
            #print('Event Code {} not in data frame'.format(eventcode))
            return df
        else:
            df.loc[df.EventCode==eventcode, new_field_name] = df.loc[df.EventCode==eventcode, copy_field]
    
    if apply_to_timestamp=='all':
        df[new_field_name] = df.groupby(['SignalID','Timestamp'], group_keys=False)[new_field_name].transform('max') ## This seems to work
    elif apply_to_timestamp=='group':
        group_vars = list(set(['SignalID','Timestamp'] + group_fields))
        df[new_field_name] = df.groupby(group_vars, group_keys=False)[new_field_name].transform('max') ## This seems to work
    
    if off_eventcode is not None:
        df.loc[df.EventCode==off_eventcode, new_field_name] = -1
    
    if direction == 'down':
        df[new_field_name] = df.groupby(group_fields)[new_field_name].ffill()
    elif direction == 'up':
        df[new_field_name] = df.groupby(group_fields)[new_field_name].bfill()

    if off_eventcode is not None:
        df.loc[df[new_field_name]==-1, new_field_name] = None
    
    return df


def copy_down(
        df, eventcodes, new_field_name, group_fields, copy_field, 
        off_eventcode=None, 
        apply_to_timestamp='all'):
    return copy_updown(
            df, eventcodes, new_field_name, group_fields, copy_field, 
            off_eventcode=off_eventcode, 
            apply_to_timestamp=apply_to_timestamp,
            direction='down')


def copy_up(
        df, eventcodes, new_field_name, group_fields, copy_field, 
        off_eventcode=None, 
        apply_to_timestamp='all'):
    return copy_updown(
            df, eventcodes, new_field_name, group_fields, copy_field, 
            off_eventcode=off_eventcode, 
            apply_to_timestamp=apply_to_timestamp,
            direction='up')


def widen(s, date_, det_config=None):
    
    config_bucket = 'gdot-devices'
    events_bucket = 'gdot-spm'
    signalid = s
    if type(date_) != str:
        date_str = date_.strftime('%Y-%m-%d')
    else:
        date_str = date_
    
    print('{} | {} started.'.format(date_str, s))

    if det_config is None:
        det_config = get_det_config(config_bucket, date_str)
    
    dc = det_config[['SignalID', 'Detector', 'CallPhase','TimeFromStopBar']]
    
    #df = get_eventlog_data(events_bucket, signalid, date_str)
    df = get_eventlog_data_db(signalid, date_str)
    
    df = df.rename(columns={'TimeStamp':'Timestamp'})
    df = df.sort_values(['SignalID','Timestamp','EventCode','EventParam']).reset_index(drop=True)
    
    print('{} | {} data queried from MaxView.'.format(date_str, s))
    
    # Map Detectors to Phases. 
    # Replace EventParam with Phase to align with other event types
    # Add new field for Detector from original EventParam field
    detector_codes = list(range(81,89))
    dc2 = pd.concat([dc.assign(EventCode=d).rename(columns={'Detector':'EventParam'}) for d in detector_codes])
    
    df = pd.merge(
            left=df, 
            right=dc2, 
            on=['SignalID','EventCode','EventParam'], 
            how='left')\
        .reset_index(drop=True)
    # Adjust Timestamp for detectors by adding TimeFromStopBar
    df.Timestamp = df.Timestamp + pd.to_timedelta(df.TimeFromStopBar.fillna(0), 's')
    df = df.sort_values(['SignalID','Timestamp','EventCode','EventParam'])
    # Rename Detector, Phase columns
    df.loc[df.EventCode.isin(detector_codes), 'Detector'] = df.loc[df.EventCode.isin(detector_codes), 'EventParam']
    df.loc[df.EventCode.isin(detector_codes), 'Phase'] = df.loc[df.EventCode.isin(detector_codes), 'CallPhase']
    df = df.drop(columns=['CallPhase','TimeFromStopBar'])
    
    df = create_new_grouping_field(df, list(range(83,89)), ['SignalID', 'Detector'], 'DetectorFault')
    df = copy_down(df, list(range(84,89)), 'DetectorFault', ['SignalID','Detector'], 'EventParam', off_eventcode=83)
    
    ped_input_codes = [89, 90]
    df.loc[df.EventCode.isin(ped_input_codes), 'PedInput'] = df.loc[df.EventCode.isin(ped_input_codes), 'EventParam']
    
    # Global (Signal-wide) copy-downs. Uses two-step function to create new field and copy down
    df = copy_down(df, 31, 'Ring', ['SignalID'], 'EventParam')
    df = copy_down(df, 31, 'CycleStart', ['SignalID'], 'Timestamp')
    df = copy_down(df, 131, 'CoordPattern', ['SignalID'], 'EventParam')
    df = copy_down(df, 132, 'CycleLength', ['SignalID'], 'EventParam')
    df = copy_down(df, 133, 'CycleOffset', ['SignalID'], 'EventParam')
    df = copy_down(df, 150, 'CoordState', ['SignalID'], 'EventParam')
    df = copy_down(df, 173, 'FlashStatus', ['SignalID'], 'EventParam')
    
    
    phase_eventcodes = list(range(0,25)) + list(range(41,50)) + [151]
    df = create_new_grouping_field(df, phase_eventcodes, 'EventParam', 'Phase')
    df = copy_down(
            df, 
            eventcodes=[0], 
            new_field_name='PhaseStart', 
            group_fields=['SignalID','Phase'], 
            copy_field='Timestamp')
    df = copy_down(df, [1,8,10], 'Interval', ['SignalID','Phase'], 'EventCode', apply_to_timestamp='group')
    
    
    # TODO: See if we can get mapping between Vehicle Detector ID and Phase using (82, 81) and (43, 44).
    #       Seems we can.
    # TODO: See if we can get mapping between Pedestrian Detector ID and Phase using (90), (45)
    
    split_eventcodes = list(range(134,150))
    df = create_new_grouping_field(df, split_eventcodes, 'EventCode', 'Phase', lambda x: x-133)
    df = copy_down(df, split_eventcodes, 'ProgrammedSplit', ['SignalID','Phase'], 'EventParam', apply_to_timestamp='group')
    
    split_eventcodes = list(range(300, 316))
    df = create_new_grouping_field(df, split_eventcodes, 'EventCode', 'Phase', lambda x: x-299)
    df = copy_up(df, split_eventcodes, 'RecordedSplit', ['SignalID','Phase'], 'EventParam', apply_to_timestamp='group')
    
    
    df = copy_down(df, [183, 184], 'PowerFailure', ['SignalID'], 'EventParam', off_eventcode=182)
    
    # Pair up detector on/offs under eventcode 82
    df = copy_up(df, 81, 'DetectorOff', ['SignalID','Detector'], 'Timestamp', apply_to_timestamp=None)
    df.loc[df.EventCode != 82, 'DetectorOff'] = np.nan
    df['DetectorDuration'] = (df['DetectorOff'] - df['Timestamp'])/pd.Timedelta(1, 's')
    
    # Pair up ped input on/offs under eventcode 90
    df = copy_up(df, 89, 'PedInputOff', ['SignalID','Detector'], 'Timestamp', apply_to_timestamp=None)
    df.loc[df.EventCode != 90, 'PedInputOff'] = np.nan
    df['PedInputDuration'] = (df['PedInputOff'] - df['Timestamp'])/pd.Timedelta(1, 's')
    
    # Pair up phase call on/offs under eventcode 43
    #df = copy_up(df, 44, 'PhaseCallOff', ['SignalID','Detector'], 'Timestamp', apply_to_timestamp=None)
    #df.loc[df.EventCode != 43, 'PhaseCallOff'] = np.nan
    #df['PhaseCallDuration'] = (df['PhaseCallOff'] - df['Timestamp'])/pd.Timedelta(1, 's')
    df = df[~df.EventCode.isin([43])]
    df = df[~df.EventCode.isin([44,81,89])]
    
    # TODO: Need a way to account for multiple detectors, inputs, etc. that overlap.
    #       Add a new column for each? e.g., Detector1, Detector2, etc.?
    
    # Possible update to copy_down for phase interval status. but needs to be grouped by phase
    #df.loc[df.EventCode.isin(eventcodes), 'Interval'] = df.loc[df.EventCode.isin(eventcodes), 'EventCode']
    
    #    csv_filename = 'c:/users/alan.toppen/documents/temp/events_{s}_{d}.csv.zip'.format(
    #            d=date_str, s=signalid)
    #    df.to_csv(csv_filename, compression='zip')
    
    print('{} | {} done.'.format(date_str, s))
    
    df.to_parquet('s3://{b}/atspm_wide/date={d}/atspm_wide_{s}_{d}.parquet'.format(
            b=events_bucket, d=date_str, s=s))
    
    return df


def get_signalids(bucket, prefix):

    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects')

    # Create a PageIterator from the Paginator
    page_iterator = paginator.paginate(
        Bucket=bucket, 
        Prefix=prefix)

    for contents in [page['Contents'] for page in page_iterator]:
        keys = [content['Key'] for content in contents]
        for key in keys:
            try:
                signalid = re.search('atspm_(.+?)_', key).group(1)
            except AttributeError:
                signalid = ''

            yield signalid
            

if __name__=='__main__':

    if len(sys.argv) > 1:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    else:
        start_date = '2020-02-24'
        end_date = '2020-02-24'
        #sys.exit('Need start_date and end_date as command line parameters')
    
    if start_date == 'yesterday': 
        start_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    if end_date == 'yesterday': 
        end_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    
    #date_str = '2020-01-23'
    #date_ = date_str
    
    # start_date = '2020-02-01'
    # end_date = '2020-02-01'

    dates = pd.date_range(start_date, end_date, freq='1D')
    
    for date_ in dates:
        date_str = date_.strftime('%Y-%m-%d')
        print(date_str)
        
        signalids = get_signalids(events_bucket, prefix='atspm/date={}'.format(date_str))
        signalids = ["3215126"]
        """
        signalids = ['10000', '10002', '10003', '10004', '10005', '10006', '10007',
       '10008', '10009', '10010', '10011', '10012', '9801', '9802',
       '9803', '9804', '9805', '9806', '9807', '9808', '9809', '9810',
       '9811', '9812', '9813', '9814', '9816', '9817', '9818', '9819',
       '9820', '9821', '9822', '9823', '9824', '9825', '9826', '9829',
       '9830', '9831', '9833', '9834', '9835', '9836', '9837', '9838',
       '9839', '9840', '9841', '9842', '9844', '9845', '9846', '9847',
       '9848', '9850', '9852', '9853', '9855', '9856', '9857', '9858',
       '9859', '9860', '9861', '9862', '9863', '9864', '9865', '9866',
       '9867', '9868', '9869', '9870', '9871', '9872', '9873', '9874',
       '9876', '9877', '9878', '9879', '9880', '9881', '9882', '9883',
       '9884', '9886', '9888', '9889', '9890', '9891', '9892', '9893',
       '9894', '9895', '9896', '9897', '9898', '9900', '9901', '9902',
       '9903', '9904', '9905', '9906', '9907', '9908', '9909', '9910',
       '9911', '9912', '9913', '9914', '9915', '9916', '9917', '9918',
       '9919', '9920', '9921', '9922', '9923', '9924', '9925', '9926',
       '9927', '9928', '9929', '9930', '9931', '9932', '9933', '9934',
       '9935', '9936', '9937', '9938', '9939', '9940', '9941', '9942',
       '9943', '9944', '9945', '9946', '9947', '9948', '9949', '9950',
       '9951', '9952', '9953', '9954', '9955', '9956', '9958', '9959',
       '9960', '9961', '9962', '9963', '9965', '9966', '9967', '9968',
       '9969', '9970', '9971', '9972', '9973', '9974', '9975', '9976',
       '9977', '9978', '9979', '9980', '9981', '9982', '9989', '9998',
       '9991', '9992', '9993', '9995', '9996', '9997']
        """
        det_config = get_det_config(config_bucket, date_str)
        
        with Pool(8) as pool:
            pool.starmap_async(widen, list(itertools.product(signalids, [date_], [det_config])))
            pool.close()
            pool.join()
