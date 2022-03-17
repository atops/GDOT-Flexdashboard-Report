# -*- coding: utf-8 -*-
"""
Created on Sun Feb  3 17:22:39 2019

@author: V0010894
"""

# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from multiprocessing.dummy import Pool
import pandas as pd
import sqlalchemy as sq
import pyodbc
import time
import os
import itertools
import boto3
import yaml
import feather
import io
import zipfile
from config import get_date_from_string

s3 = boto3.client('s3')
ath = boto3.client('athena')


'''
    df:
        SignalID [int64]
        TimeStamp [datetime]
        EventCode [str or int64]
        EventParam [str or int64]
    
    det_config:
        SignalID [int64]
        IP [str]
        PrimaryName [str]
        SecondaryName [str]
        Detector [int64]
        Call Phase [int64]
'''

def pull_raw_atspm_data(s, date_):
    
    try:
        if s in GroupableElements_Map.SignalID:
            did = GroupableElements_Map[GroupableElements_Map.SignalID==s].DeviceId.values[0]
            #dc_fn = '../ATSPM_Det_Config_Good_{}.feather'.format(date_.strftime('%Y-%m-%d'))
            #det_config = (feather.read_dataframe(dc_fn)
            #                .assign(SignalID = lambda x: x.SignalID.astype('int64'))
            #                .assign(Detector = lambda x: x.Detector.astype('int64'))
            #                .rename(columns={'CallPhase':'Call Phase'}))
            #
            #left = det_config[det_config.SignalID==s]
            #right = bad_detectors[(bad_detectors.SignalID==s) & (bad_detectors.Date==date_)]
                
            #det_config_good = (pd.merge(left, right, how = 'outer', indicator = True)
            #                     .loc[lambda x: x._merge=='left_only']
            #                     .drop(['Date','_merge'], axis=1))
            
            #sum(~pd.isnull(det_config_good['CallPhase.atspm']))
            monday = (date_ - pd.DateOffset(days=(date_.weekday()))).strftime('%m-%d-%Y')
            
            query1 = """SELECT * FROM [ASC_PhasePed_Events_{}]
                       WHERE DeviceID = '{}'
                       AND EventId in (1,4,5,6,8,9,31) 
                       AND (TimeStamp BETWEEN '{}' AND '{}');
                       """
            query2 = """SELECT * FROM [ASC_Det_Events_{}]
                       WHERE DeviceID = '{}'
                       AND EventId in (81,82,89,90) 
                       AND (TimeStamp BETWEEN '{}' AND '{}');
                       """
            start_date = date_
            end_date = date_ + pd.DateOffset(days=1) - pd.DateOffset(seconds=0.1)
            
            
            t0 = time.time()
            date_str = date_.strftime('%Y-%m-%d') #str(date_)[:10]
            print('{} | {} Starting...'.format(s, date_str))
        
            try:
                print('|{} reading from database...'.format(s))
                with mv_el_engine.connect() as conn:
                    df = pd.read_sql(sql=query1.format(monday, did, str(start_date)[:-3], str(end_date)[:-3]), con=conn)
                    df1 = df.assign(SignalID = s)
                    
                with mv_el_engine.connect() as conn:
                    df = pd.read_sql(sql=query2.format(monday, did, str(start_date)[:-3], str(end_date)[:-3]), con=conn)
                    df2 = (df.assign(SignalID = s))
                    
                df = (pd.concat([df1, df2])
                        .rename(columns = {'TimeStamp': 'Timestamp', 
                                           'EventId': 'EventCode', 
                                           'Parameter': 'EventParam'})
                        .sort_values(['SignalID','Timestamp','EventCode','EventParam']))
                
                if len(df) == 0:
                    print('|{} no event data for this signal on {}.'.format(s, date_str))
        
                else:
            
                    print('writing to files...{} records'.format(len(df)))
                    
                    if not os.path.exists('../atspm/' + date_str):
                        os.mkdir('../atspm/' + date_str)
                        
                    parquet_filename = '../atspm/{}/atspm_{}_{}.parquet'.format(date_str, s, date_str)
                    #feather_filename = parquet_filename.replace('.parquet','.feather')
                    df.to_parquet(parquet_filename)
                    
                    #with io.StringIO() as buff:
                    #    feather.write_dataframe(df, buff)
                    #    with zipfile.ZipFile(feather_filename + '.zip', mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
                    #        zf.write(os.path.basename(feather_filename), buff.getvalue())
                    #feather.write_dataframe(df, feather_filename)
                    
                    s3.upload_file(Filename = parquet_filename, 
                                   Bucket = conf['bucket'],
                                   Key = 'atspm/date={}/atspm_{}_{}.parquet'.format(date_str, s, date_str))
                    
                    os.remove(parquet_filename)
                    
            
                    print('{}: {} seconds'.format(s, int(time.time()-t0)))
                
            
            except Exception as e:
                print(s, e)

    except Exception as e:
        print(s, e)

        
        
    
if __name__=='__main__':

    if os.name=='nt':
        
        uid = os.environ['ATSPM_USERNAME']
        pwd = os.environ['ATSPM_PASSWORD']
        
        mv_el_engine = sq.create_engine('mssql+pyodbc://{}:{}@MaxView_EventLog'.format(uid, pwd), pool_size=20)
        mv_engine = sq.create_engine('mssql+pyodbc://{}:{}@MaxView'.format(uid, pwd), pool_size=20)
        atspm_engine = sq.create_engine('mssql+pyodbc://{}:{}@atspm'.format(uid, pwd), pool_size=20)
    
    elif os.name=='posix':

        def connect():
            return pyodbc.connect(
                'DRIVER=FreeTDS;' + 
                'SERVER={};'.format(os.environ["ATSPM_SERVER_INSTANCE"]) +
                'DATABASE={};'.format(os.environ["ATSPM_DB"]) +
                'UID={};'.format(os.environ['ATSPM_USERNAME']) +
                'PWD={};'.format(os.environ['ATSPM_PASSWORD']) +
                'TDS_Version=8.0;')
        
        engine = sq.create_engine('mssql://', creator=connect)
        
    with atspm_engine.connect() as conn:
        Signals = pd.read_sql_table('Signals', conn)
    
    with mv_engine.connect() as conn:
        GroupableElements = (pd.read_sql_table('GroupableElements', conn)
                               .assign(SignalID = lambda x: x.Number.astype('int64'),
                                       DeviceId = lambda x: x.ID.astype('int64')))
        GroupableElements_IntersectionController = pd.read_sql_table('GroupableElements_IntersectionController', conn)
    
        GroupableElements_Map = pd.merge(GroupableElements_IntersectionController[['ID','Intersection_Name']], 
                                         GroupableElements[['SignalID','DeviceId','Name']], 
                                         left_on=['ID'], 
                                         right_on=['DeviceId'], 
                                         how = 'inner')

    #bad_detectors = (feather.read_dataframe('bad_detectors.feather')
    #                    .assign(SignalID = lambda x: x.SignalID.astype('int64'),
    #                            Detector = lambda x: x.Detector.astype('int64')))
    
    #corridors = pd.read_feather("GDOT-Flexdashboard-Report/corridors.feather")
    #signalids = list(corridors.SignalID.astype('int').values)
    
    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file)

    start_date = conf['start_date']
    end_date = conf['end_date']
    
    start_date = get_date_from_string(start_date)
    end_date = get_date_from_string(end_date)

    # Placeholder for manual override of start/end dates
    #start_date = '2019-02-01'
    #end_date = '2019-02-03'
    
    dates = pd.date_range(start_date, end_date, freq='1D')
                                        
    #corridors_filename = conf['corridors_filename']
    #corridors = feather.read_dataframe(corridors_filename)
    #corridors = corridors[~corridors.SignalID.isna()]
    
    #signalids = list(corridors.SignalID.astype('int').values)
    
    signalids = list(Signals[Signals.SignalID != 'null'].SignalID.astype('int').values)
    
    for date_ in dates:

        t0 = time.time()

        pool = Pool(18) #24
        asyncres = pool.starmap(pull_raw_atspm_data, list(itertools.product(signalids, [date_])))
        pool.close()
        pool.join()
    

    
        os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
        
        partition_query = '''ALTER TABLE atspm2 add partition (date="{d}") 
                             location "s3://{b}/atspm/date={d}/"'''.format(b=conf['bucket'], d=date_.date())
        
        response = ath.start_query_execution(QueryString = partition_query, 
                                             QueryExecutionContext={'Database': conf['bucket']},
                                             ResultConfiguration={'OutputLocation': conf['athena']['staging_dir']})
        print(response)
        
    print('\n{} signals in {} days. Done in {} minutes'.format(len(signalids), len([date_]), int((time.time()-t0)/60)))

