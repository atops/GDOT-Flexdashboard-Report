# -*- coding: utf-8 -*-
"""
Created on Sat Aug 24 20:04:51 2019

@author: V0010894
"""

import io
import pandas as pd
import boto3
import requests
from datetime import datetime
from multiprocessing import Pool


def get_rsus_dict(bucket, key):
    """
    Get list of dictionaries of RSU attributes from Excel file on S3
    
    args:
            bucket: S3 Bucket of RSU attributes Excel file
            key: S3 Key of RSU attributes Excel file
    returns:
            list of dictionaries of RSU attributes from the Excel file
    """

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    obj = bucket.Object(key)
    
    with io.BytesIO() as f:
        obj.download_fileobj(f)
        rsus = pd.read_excel(f, sheet_name=0)
        #f.seek(0)
        #df1 = pd.read_excel(f, sheet_name=1)
    
    #rsus = pd.concat([df0, df1], sort=True)
    rsus = rsus[~rsus['RSU IP Address'].isna()]
    rsus.SignalID = rsus.SignalID.astype('int')
    
    return rsus.to_dict(orient='Records')
    

def rsu_main(rsu_dict):
    """
    Get http headers for RSU IP address. This only indicates whether it is up,
    and says nothing about whether it is transmitting data.
    
    args:
            rsu_dict: Dictionary of RSU attributes from Excel file
                      minimum attributes required: 'SignalID', 'RSU IP Address'
    returns:
            dictionary of headers for RSU defined by 'SignalID', 'RSU IP Address'
    """
    
    try:
        response = requests.get('http://{ip}'.format(ip=rsu_dict['RSU IP Address']), timeout=2)
        headers = response.headers
        headers['Up'] = True
    except:
        headers = dict()
        headers['Up'] = False
    finally:
        headers['SignalID'] = rsu_dict['SignalID']
        headers['Timestamp'] = pd.Timestamp.now().floor('s')
        #print(headers)
        #print('-----')
        return headers


def main():
    
    rsus_dict = get_rsus_dict(bucket='gdot-spm', key='GDOT_RSU.xlsx') 
    
    with Pool(12) as pool:
        results = pool.map_async(rsu_main, rsus_dict)
        pool.close()
        pool.join()

    headers = results.get()
    return pd.DataFrame(headers)
    
    #    for rsu in rsus_dict:
    #        rsu_main(rsu)
        
if __name__=='__main__':

    now = datetime.now()

    headers = main()
    headers.to_parquet(
        's3://gdot-spm/mark/rsus/date={}/rsus_{}.parquet'.format(
                now.strftime('%Y-%m-%d'),
                now.strftime('%Y%m%d%H%M%S')))
    
    success = sum(headers['Up'])
    total = len(headers)
    now = now.strftime('%Y-%m-%d %H:%M:%S')
    print('{} - {} out of {} succeeded'.format(now, success, total))