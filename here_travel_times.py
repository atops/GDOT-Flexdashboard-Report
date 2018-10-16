# -*- coding: utf-8 -*-\
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import requests
import uuid

df = pd.read_feather('c:/Users/alan.toppen/Code/GDOT/GDOT-Flexdashboard-Report/tmc_routes.feather')

df.groupby(['corridor'])['tmc'].apply(list).to_dict()


uri = 'http://kestrel.ritis.org:8080/{}'
key = '3e044c6947c947a6a09d78d08f942577'
params = {'key': key}
#----------------------------------------------------------  
request = {
  "dates": [
    {
      "end": "2017-07-01",
      "start": "2017-06-30"
    }
  ],
  "dow": [
    0,
    1,
    2,
    3,
    4,
    5,
    6
  ],
  "dsFields": [
    {
      "columns": [
        "SPEED",
        "AVERAGE_SPEED",
        "REFERENCE_SPEED",
        "TRAVEL_TIME_MINUTES",
        "CVALUE",
        "CONFIDENCE_SCORE"
      ],
      "dataSource": "vpp_inrix",
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
    "value": 0
  },
  "times": [
    {
      "end": "12:00:00.000",
      "start": "00:00:00.000"
    }
  ],
  "tmcs": [
    "110-04603",
    "110-04604",
    "110-04605"
  ],
  "travelTimeUnits": "SECONDS",
  "uuid": str(uuid.uuid1())
}
#----------------------------------------------------------  
payload = {
  "dataSource": "vpp_here",
  "dates": [
    {
      "start": "2018-09-01",
      "end": "2018-09-30"
    }
  ],
  "groupTmcs": [
    {
      "alias": "CLOCKWISE",
      "tmcs": [
        "101+10797",
        "101+10798",
        "101P10798"
      ]
    }
  ],
  "percentiles": [
    25,
    50,
    75,
    100
  ],
  "requestIntervals": [
    {
      "dateRange": {
        "start": "2018-09-01",
        "end": "2018-10-01"
      },
      "dow": [
        2,
        3,
        4
      ],
      "granularity": {
        "type": "minutes",
        "value": 60
      },
      "intervalName": "weekdays",
      "periodId": 0,
      "useDayofWeekIntervalName": 'false'
    }
  ],
  "times": [
    {
      "start": "00:00:00.000",
      "end": "12:00:00.000"
    }
  ],
  "uuid": str(uuid.uuid1())
}
  
response = requests.post(uri.format('jobs/export'), 
                         params = params, 
                         json = request)
response.status_code

results = requests.get(uri.format('jobs/export/results'), 
                       params = {'key': key, 'uuid': request['uuid']})

with open('ritis.zip', 'wb') as f:
    f.write(results.content)
    