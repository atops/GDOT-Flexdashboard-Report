# -*- coding: utf-8 -*-
"""
Created on Mon Sep 24 20:42:51 2018

@author: Alan.Toppen
"""

# -*- coding: utf-8 -*-\
"""
Spyder Editor

This is a temporary script file.
"""

import os
import pandas as pd
import requests
import uuid
import polling
import time
import yaml
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
#----------------------------------------------------------  
def get_tmc_data(start_date, end_date, tmcs, initial_sleep_sec=0):
    
    # Allow sleep time to space out requests when running in a loop
    time.sleep(initial_sleep_sec)

    uri = 'http://kestrel.ritis.org:8080/{}'
    key = '3e044c6947c947a6a09d78d08f942577'
    
    payload = {
      "dates": [
        {
          "end": end_date,
          "start": start_date
        }
      ],
      "dow": [
        2,
        3,
        4
      ],
      "dsFields": [
        {
          "columns": [
            "SPEED",
            "REFERENCE_SPEED",
            "TRAVEL_TIME_MINUTES",
            "CONFIDENCE_SCORE"
          ],
          "dataSource": "vpp_here",
          "qualityFilter": {
            "max": 1,
            "min": 0,
            "thresholds": [
              30,
              20,
              10
            ]
          }
        }
      ],
      "granularity": {
        "type": "minutes",
        "value": 60
      },
      "times": [
        {
          "end": None,
          "start": "00:00:00.000"
        }
      ],
      "tmcs": tmcs,
      "travelTimeUnits": "SECONDS",
      "uuid": str(uuid.uuid1())
    }  
    #----------------------------------------------------------    
    response = requests.post(uri.format('jobs/export'), 
                             params = {'key': key}, 
                             json = payload)
    print('response status code:', response.status_code)
    
    if response.status_code == 200: # Only if successful response

        # retry at intervals of 'step' until results return (status code = 200)
        results = polling.poll(
                lambda: requests.get(uri.format('jobs/export/results'), 
                               params = {'key': key, 'uuid': payload['uuid']}),
                check_succsss = lambda x: x.status_code == 200,
                step = 10,
                poll_forever = True
                )
        print('results received')
        
        # Save results (binary zip file with one csv)
        zf_filename = payload['uuid'] +'.zip'
        with open(zf_filename, 'wb') as f:
            f.write(results.content)
        df = pd.read_csv(zf_filename)
        os.remove(zf_filename)
    
    else:
        df = pd.DataFrame()
    
    return df

if __name__=='__main__':

    
    with open('Monthly_Report_calcs.yaml') as yaml_file:
        conf = yaml.load(yaml_file)

    start_date = conf['start_date']
    if start_date == 'yesterday': 
        start_date = datetime.today() - timedelta(days=1)
        start_date = (start_date - timedelta(days=(start_date.day - 1))).strftime('%Y-%m-%d')
    end_date = conf['end_date']
    if end_date == 'yesterday': 
        end_date = datetime.today().strftime('%Y-%m-%d')


    tmc_fn = 'tmc_routes.feather'
    tmc_df = pd.read_feather(tmc_fn)
    tmc_dict = tmc_df.groupby(['corridor'])['tmc'].apply(list).to_dict()
    
    
    #start_date = '2018-09-16'
    #end_date = '2018-09-25'
    
    df = pd.DataFrame()
    for corridor in tmc_dict.keys():
        print(corridor)
        tt_df = get_tmc_data(start_date, end_date, tmc_dict[corridor], 10)
    
        if len(tt_df) > 0:
            df_ = (pd.merge(tt_df, tmc_df[['tmc','miles']], left_on=['tmc_code'], right_on=['tmc'])
                     .drop(columns=['tmc'])
                     .assign(corridor = str(corridor)))
            df = pd.concat([df, df_])
            
    if len(df) > 0:
        df = df.reset_index(drop=True)
        fn = 'tt_{}.feather'.format(start_date.replace('-01',''))
        print(fn)
        df.to_feather(fn)
        

