# -*- coding: utf-8 -*-
"""
Created on Sun Feb  3 17:22:39 2019

@author: V0010894
"""

# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
#from multiprocessing.dummy import Pool
from multiprocessing import Pool
import pandas as pd
import sqlalchemy as sq
import pyodbc
import sys
import time
import os
import re
import itertools
import boto3
import yaml
import pprint
#import feather
#import io
#import zipfile

s3 = boto3.client('s3')
ath = boto3.client('athena')
ec2 = boto3.client('ec2')

pp = pprint.PrettyPrinter()

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

def pull_raw_atspm_data(s, date_, GroupableElements):

    try:

        if s in GroupableElements.SignalID.values: # and not already_done:
            dids = GroupableElements[GroupableElements.SignalID==s].DeviceId.values
            for did in dids:

                monday = (date_ - pd.DateOffset(days=(date_.weekday()))).strftime('%m-%d-%Y')

                query1 = """SELECT * FROM [ASC_PhasePed_Events_{}]
                           WHERE DeviceID = '{}'
                           AND EventId in (1,4,5,6,8,9,31,21,22,45) 
                           AND (TimeStamp BETWEEN '{}' AND '{}');
                           """
                query2 = """SELECT * FROM [ASC_Det_Events_{}]
                           WHERE DeviceID = '{}'
                           AND EventId in (81,82,89,90) 
                           AND (TimeStamp BETWEEN '{}' AND '{}');
                           """
                query3 = """SELECT * FROM [ASC_Coord_Events_{}]
                           WHERE DeviceID = '{}'
                           AND EventId in (132) 
                           AND (TimeStamp BETWEEN '{}' AND '{}');
                           """
                start_date = date_
                end_date = date_ + pd.DateOffset(days=1) - pd.DateOffset(seconds=0.1)


                t0 = time.time()
                date_str = date_.strftime('%Y-%m-%d') #str(date_)[:10]
                print('{}|{} Starting...'.format(s, date_str))

                try:
                    #print('|{} reading from database...'.format(s))
                    muid = os.environ['MAXV_USERNAME']
                    mpwd = os.environ['MAXV_PASSWORD']

                    mv_el_engine = sq.create_engine('mssql+pyodbc://{}:{}@MaxView_EventLog'.format(muid, mpwd))

                    with mv_el_engine.connect() as conn:
                        df = pd.read_sql(
                                sql=query1.format(monday, 
                                                  did, 
                                                  re.sub('\d{3}$','', str(start_date)), 
                                                  re.sub('\d{3}$','', str(end_date))),
                                con=conn)
                        df1 = df.assign(SignalID = s)

                    with mv_el_engine.connect() as conn:
                        df = pd.read_sql(
                                sql=query2.format(monday, 
                                                  did, 
                                                  re.sub('\d{3}$','', str(start_date)), 
                                                  re.sub('\d{3}$','', str(end_date))), 
                                con=conn)
                        df2 = (df.assign(SignalID = s))

                    with mv_el_engine.connect() as conn:
                        df = pd.read_sql(
                                sql=query3.format(monday, 
                                                  did, 
                                                  re.sub('\d{3}$','', str(start_date)), 
                                                  re.sub('\d{3}$','', str(end_date))), 
                                con=conn)
                        df3 = (df.assign(SignalID = s))

                    df = pd.concat([df1, df2, df3]).drop_duplicates()

                    if len(df) == 0:
                        print('{}|No event data for this signal on {}.'.format(s, date_str))

                    else:
                        df = df.rename(columns = {'TimeStamp': 'Timestamp', 
                                                  'EventId': 'EventCode', 
                                                  'Parameter': 'EventParam'})\
                            .sort_values(['SignalID','Timestamp','EventCode','EventParam'])
                        # New on 8/21/19
                        # Removed on 8/22. Need time zone naive files on parquet s3 for athena
                        # Need to account for time zone on retrieval, esp. in R
                        #df.Timestamp = df.Timestamp.dt.tz_localize('America/New_York') 

                        #print('{}|writing to files...{} records'.format(s, len(df)))
                        df.to_parquet('s3://gdot-spm/atspm/date={}/atspm_{}_{}.parquet'.format(date_str, s, date_str))
						
                        print('{}|{} records|{} seconds'.format(
                                s, len(df), round(time.time()-t0, 1)))

                except Exception as e:
                    print(s, e)

    except Exception as e:
        print(s, e)




if __name__=='__main__':

    uid = os.environ['ATSPM_USERNAME']
    pwd = os.environ['ATSPM_PASSWORD']

    muid = os.environ['MAXV_USERNAME']
    mpwd = os.environ['MAXV_PASSWORD']

    mv_engine = sq.create_engine('mssql+pyodbc://{}:{}@MaxView'.format(muid, mpwd))
    atspm_engine = sq.create_engine('mssql+pyodbc://{}:{}@atspm'.format(uid, pwd))


    with atspm_engine.connect() as conn:
        Signals = pd.read_sql_table('Signals', conn)

    with mv_engine.connect() as conn:
        GroupableElements = (pd.read_sql_table('GroupableElements', conn)
                               .assign(SignalID = lambda x: x.Number.astype('int64'),
                                       DeviceId = lambda x: x.ID.astype('int64')))

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.FullLoader)


    if len(sys.argv)==3:
        start_date = sys.argv[1]
        end_date = sys.argv[2]

    elif len(sys.argv)==2:
        start_date = sys.argv[1]
        end_date = sys.argv[1]

    elif len(sys.argv)==1:
        start_date = conf['start_date']
        if start_date == 'yesterday': 
            start_date = datetime.today().date() - timedelta(days=1)
            while True:
                response = s3.list_objects_v2(
                        Bucket = "gdot-spm", 
                        Prefix = "atspm/date={}".format(start_date.strftime('%Y-%m-%d')))
                if response['KeyCount'] > 0:
                    start_date = (start_date + timedelta(days=1)) #.strftime('%Y-%m-%d')
                    break
                else:
                    start_date = start_date - timedelta(days=1)
            start_date = min(start_date, datetime.today().date() - timedelta(days=1))
        end_date = conf['end_date']
        if end_date == 'yesterday': 
            end_date = (datetime.today().date() - timedelta(days=1)) #.strftime('%Y-%m-%d')

    else:
        sys.exit("Too many command line arguments")

    # Placeholder for manual override of start/end dates
    #start_date = '2019-12-18'
    #end_date = '2019-11-09'

    dates = pd.date_range(start_date, end_date, freq='1D')

    # Unique signalids
    signalids = list(set(
            Signals[Signals.SignalID != 'null'].SignalID.astype('int').values
            ))

    t0 = time.time()
    for date_ in dates:

        with Pool(processes=os.cpu_count() * 6) as pool: #18
            pool.starmap_async(pull_raw_atspm_data, list(itertools.product(signalids, [date_], [GroupableElements])))
            pool.close()
            pool.join()

        os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

        partition_query = '''ALTER TABLE atspm2 add partition (date="{0}") 
                             location "s3://gdot-spm/atspm/date={0}/"'''.format(date_.date())

        response = ath.start_query_execution(QueryString = partition_query, 
                                             QueryExecutionContext={'Database': 'gdot_spm'},
                                             ResultConfiguration={'OutputLocation': 's3://gdot-spm-athena'})
        print('Update Athena partitions:')
        pp.pprint(response)

    print('\n{} signals in {} days. Done in {} minutes'.format(len(signalids), len(dates), int((time.time()-t0)/60)))

    print(datetime.now().strftime('%c'), '| Starting EC2 instance')


    # Set instance type. The further into the month, the beefier server needed
    if end_date.day < 16:  # 10:
        instance_type = 'r5.large'
    else:
        instance_type = 'r5.xlarge'
    #elif end_date.day < 25:
    #    instance_type = 'r5.xlarge'
    #else:
    #    instance_type = 'r5.2xlarge'

    response = ec2.modify_instance_attribute(
        DryRun=False,
        InstanceId='i-0c511a8ad28a24507',
        InstanceType={'Value': instance_type},
    )
    print('EC2 instance type: ' + instance_type)


    # Start the instance
    response = ec2.start_instances(
        InstanceIds=['i-0c511a8ad28a24507'], # ['i-0ceba00e164459c14'],  # 
        DryRun=False
    )
    print('EC2 start instance:')
    pp.pprint(response['StartingInstances'])
