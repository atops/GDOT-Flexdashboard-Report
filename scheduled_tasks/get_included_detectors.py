# -*- coding: utf-8 -*-
"""
Created on Wed Nov  7 22:20:12 2018

@author: V0010894
"""

import pandas as pd
import yaml
import os
import sqlalchemy as sq
from datetime import datetime, timedelta
import pyodbc
import time
from glob import glob
import boto3
from multiprocessing import Pool
from dask import delayed, compute

s3 = boto3.client('s3')

with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.FullLoader)

start_date = (datetime.today() - timedelta(days=180)).strftime('%Y-%m-%d')
end_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
dates = pd.date_range(start_date, end_date, freq='3D')
        
query = """SELECT DISTINCT SignalID, EventParam as Detector
           FROM Controller_Event_Log WHERE EventCode = 82
           AND Timestamp between '{} 08:00:00' and '{} 09:00:00';
           """

query2 = """SELECT DISTINCT SignalID, EventParam as Detector
           FROM controller_event_log WHERE EventCode = 82
           AND Timestamp between '{} 08:00:00' and '{} 09:00:00';
           """
           
if os.name=='nt':
        
    uid = os.environ['ATSPM_USERNAME']
    pwd = os.environ['ATSPM_PASSWORD']
    
    engine = sq.create_engine('mssql+pyodbc://{}:{}@sqlodbc'.format(uid, pwd),
                              pool_size=20)
    
    mv_el_engine = sq.create_engine('mssql+pyodbc://{}:{}@MaxView_EventLog'.format(uid, pwd),
                                    pool_size=20)
    mv_engine = sq.create_engine('mssql+pyodbc://{}:{}@MaxView'.format(uid, pwd),
                                 pool_size=20)
    
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
    

def helper(date_):
    
    t0 = time.time()
            
    sd = date_.strftime('%Y-%m-%d')
    ed = sd #(date_ + pd.DateOffset(days=1)).strftime('%Y-%m-%d')
    
    print(sd, end=': ')

    with engine.connect() as conn:
        df1 = pd.read_sql(sql=query.format(sd, ed), con=conn)

    with mv_el_engine.connect() as conn2:        
        df2 = pd.read_sql(sql=query2.format(sd, ed), con=conn2)
        
    df = pd.concat([df1, df2]).drop_duplicates()
    df.SignalID = df.SignalID.astype('float').astype('int')
    
    print('{} sec'.format(time.time() - t0))

    return df
    

def get_included_detectors():
    
    #dfs = [helper(date_) for date_ in dates]
    #df = pd.concat(dfs)
    
    #results = []
    #for date_ in dates:
    #    x = delayed(helper)(date_)
    #    results.append(x)
    #df = pd.concat(compute(*results)).drop_duplicates().reset_index()
    with Pool() as pool:
        results = pool.map_async(helper, dates)
        pool.close()
        pool.join()
    df = pd.concat(results.get()).drop_duplicates().reset_index()
    
    df.to_csv('included_detectors.csv')
    feather_filename = 'included_detectors.feather'
    df.to_feather(feather_filename)
    s3.upload_file(Bucket = 'gdot-spm', 
                   Key = os.path.basename(feather_filename), 
                   Filename = feather_filename)
    #os.remove(feather_filename)

    df = (df.sort_values(['SignalID','Detector'])
            .set_index(['SignalID','Detector']))
    
    return df


if __name__=='__main__':
    
    incld = get_included_detectors()


#for fn in filenames:
#    os.remove(fn)
