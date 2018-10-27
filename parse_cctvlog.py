# -*- coding: utf-8 -*-
"""
Created on Tue Feb 20 11:10:19 2018

@author: V0010894
"""
import pandas as pd
import numpy as np
from datetime import datetime
from glob import glob
import re

from dask import delayed, compute


def parse_cctvlog(fn):
    print(fn)
    with open(fn) as f:
        json_data = f.read().strip()
    df = (pd.read_json('[{}]'.format(json_data.replace('\n',',')))
            .filter(items=['ID','Last-Modified','Content-Length'], axis=1)
            .rename(columns = {'ID': 'CameraID', 
                               'Last-Modified': 'Timestamp', 
                               'Content-Length': 'Size'})
            .assign(Timestamp = lambda x: pd.to_datetime(x.Timestamp, 
                                                         format ='%a, %d %b %Y %H:%M:%S %Z')
                                          .dt.tz_localize('UTC')
                                          .dt.tz_convert('America/New_York'))
            .assign(Date = lambda x: x.Timestamp.dt.date)
            .drop_duplicates())
    
    return df

filenames = glob('../cctvlogs/cctvlog_*.json')

today_ = datetime.today().strftime('%Y-%m-%d')
filenames = [fn for fn in filenames if re.search(today_, fn) == None]

#today_fn = '../cctvlogs/cctvlog_{}.json'.format(today_)

#if today_fn in filenames:
#    filenames.remove(today_fn)

results = []
for fn in filenames:
    x = delayed(parse_cctvlog)(fn)
    results.append(x)
dfs = compute(*results)

df = pd.concat(dfs).drop_duplicates()

#df = dfs.compute().drop_duplicates()[['ID','Last-Modified','Content-Length']]

#df = (df.rename(columns = {'ID': 'CameraID', 'Last-Modified': 'Timestamp', 'Content-Length': 'Size'})
#        .assign(Timestamp = lambda x: pd.to_datetime(x.Timestamp, format ='%a, %d %b %Y %H:%M:%S %Z'))
#        .assign(Date = lambda x: x.Timestamp.dt.date))

# Daily summary (stdev of image size for the day as a proxy for uptime: sd > 0 = working)
summ = df.groupby(['CameraID','Date']).agg(np.std).fillna(0)

summ.reset_index().to_feather('parsed_cctv.feather')