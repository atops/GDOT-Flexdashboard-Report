# -*- coding: utf-8 -*-
"""
Created on Mon Sep 24 20:42:51 2018

@author: Alan.Toppen
"""

import pandas as pd
import requests
import json
import time
import yaml
import boto3
from datetime import datetime

#os.environ['TZ'] = 'America/New_York'
#time.tzset()

s3 = boto3.client('s3')

pd.options.display.max_columns = 10


def get_tmc_codes(counties, key, initial_sleep_sec=0):

    # Allow sleep time to space out requests when running in a loop
    time.sleep(initial_sleep_sec)

    uri = 'http://kestrel.ritis.org:8080/{}'

    #----------------------------------------------------------
    #    payload_placeholder = {
    #        "county": ["Gwinnett, GA"],
    #        "countyNot": [],
    #        "dataSourceId": "vpp_inrix",
    #        "direction": [],
    #        "directionNot": [],
    #        "firstName": [],
    #        "firstNameNot": [],
    #        "nhsFClass": [],
    #        "nhsFClassNot": [],
    #        "point": {},
    #        "propertiesToReturn": ["tmc", "state", "zip"],
    #        "road": [],
    #        "roadClass": ["US Route", "State Route"],
    #        "roadClassNot": [],
    #        "roadNot": [],
    #        "state": ["GA"],
    #        "stateNot": [],
    #        "tmc": ["101-10667"],
    #        "tmcNot": [],
    #        "zip": [],
    #        "zipNot": []
    #    }

    #      "propertiesToReturn": [
    #         "tmc",
    #         "type",
    #         "roadNumber",
    #         "roadName",
    #         "firstName",
    #         "funcClass",
    #         "county",
    #         "state",
    #         "zip",
    #         "direction",
    #         "roadClass",
    #         "nhsFClass",
    #         "startLatitude",
    #         "startLongitude",
    #         "endLatitude",
    #         "endLongitude",
    #         "length",
    #         "coordinates",
    #         "linearTmc",
    #         "linearId",
    #         "roadOrder",
    #         "timezoneName"
    #      ],
    #----------------------------------------------------------
    
    Counties = [county + ", GA" for county in counties]

    payload = {
        "county": Counties,
        "roadClass": [],  #["US Route", "State Route"],
        'dataSourceId': 'vpp_here',
        'tmc': []
    }
    print(payload)

    response = requests.post(uri.format('tmc/search'),
                             params={'key': key},
                             json=payload)
    print('response status code:', response.status_code)

    if response.status_code == 200:  # Only if successful response

        tmcs_string = response.content.decode('utf-8')
        df = pd.DataFrame(json.loads(tmcs_string))

        print('results received')

    else:
        df = pd.DataFrame()

    print('{} records'.format(len(df)))
    return df


if __name__ == '__main__':

    with open('Monthly_Report_AWS.yaml') as yaml_file:
        cred = yaml.load(yaml_file, Loader=yaml.Loader)

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    counties = [
        'BARTOW', 'CHEROKEE', 'CLAYTON', 'COBB', 'DEKALB', 'DOUGLAS',
        'FAYETTE', 'FORSYTH', 'FULTON', 'GWINNETT', 'HENRY', 'PAULDING',
        'ROCKDALE',

        'APPLING', 'BACON', 'BARROW', 'BLECKLEY', 'BRANTLEY', 
        'BRYAN', 'BULLOCH', 'BURKE', 'CAMDEN', 'CANDLER', 'CATOOSA', 
        'CHARLTON', 'CHATHAM', 'CLINCH', 'DODGE', 'EFFINGHAM', 'EMANUEL', 
        'EVANS', 'FLOYD', 'GLYNN', 'GORDON', 'GREENE', 'HANCOCK', 'JASPER', 
        'JEFF DAVIS', 'JEFFERSON', 'JENKINS', 'JOHNSON', 'LIBERTY', 'LINCOLN', 
        'LONG', 'MCDUFFIE', 'MONTGOMERY', 'MORGAN', 'OGLETHORPE', 'PIERCE', 
        'PUTNAM', 'SCREVEN', 'TALIAFERRO', 'TATTNALL', 'TELFAIR', 'TOOMBS', 
        'TREUTLEN', 'WARE', 'WARREN', 'WASHINGTON', 'WAYNE', 'WHEELER', 
        'WHITFIELD', 'WILKES', 'WILKINSON'
    ]
    
    try:
        tmcs = get_tmc_codes(counties, cred['RITIS_KEY'], 1)
        
        now = datetime.now().strftime('%Y-%m-%d')

        csv_filename = 'raw_tmcs.csv'
        tmcs.reset_index(drop=True).to_csv(csv_filename, compression='zip')
        key = 'tmc_codes/date={now}/{fn}.zip'.format(now=now, fn=csv_filename)
        s3.upload_file(csv_filename, Bucket='gdot-spm', Key=key)

    except Exception as e:
        print('error retrieving records')
        print(e)
        tmcs = pd.DataFrame()
