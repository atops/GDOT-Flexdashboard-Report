# -*- coding: utf-8 -*-
"""
Created on Wed Jan 16 15:28:33 2019

@author: alan.toppen
"""
import requests
from polling import poll
import os
import yaml
from datetime import datetime
import json
import pytz
import pandas as pd
import re
import boto3
from multiprocessing import Pool
import s3io

from mark1_logger import mark1_logger


def now(tz):
    return datetime.now().astimezone(pytz.timezone(tz))


base_path = '.'

logs_path = os.path.join(base_path, 'logs')
if not os.path.exists(logs_path):
    os.mkdir(logs_path)
logger = mark1_logger(os.path.join(logs_path, f'get_cctv_{now("US/Eastern").strftime("%F")}.log'))

s3 = boto3.client('s3')


def get_camids(camids_file):
    with open(camids_file) as f:
        camids = [camid.split(': ')[0].strip() for camid in f.readlines()[1:]]
    return camids


def get_camids_csv(camids_file):
    df = pd.read_csv(camids_file.replace('xlsx','csv'))
    return list(df.CameraID.values)


def get_camids_xl(camids_file):
    df = pd.read_excel(camids_file)
    return list(df.CameraID.values)


def get_camids_s3(conf):
    camids = list(s3io.read_excel(Bucket=conf['bucket'], Key=conf['cctv_config_filename']).CameraID.values)
    return camids


def get_camids_nav():
    nav_url = 'http://navigator-c2c.dot.ga.gov/snapshots/'
    try:
        response = requests.get(nav_url)
        camids = sorted(list(set(
                re.findall("(?<=/snapshots/)(.*?)\.jpg", response.text)
            )))
        logger.info(f'Read {len(camids)} cameras from {nav_url}')
    except:
        logger.info(f'Can\'t reach {nav_url}')
        camids = []
    finally:
        return camids


def get_camera_data(camid):
    try:
        url = f'http://navigator-c2c.dot.ga.gov/snapshots/{camid}.jpg'
        response = poll(lambda: requests.get(url, timeout=2), step=2, timeout=10,
                        ignore_exceptions=(requests.exceptions.ConnectionError))

        if response.status_code == 200:

            dict_ = dict(response.headers)
            dict_['ID'] = camid
            json_data = json.dumps(dict_)

            if 'Last-Modified' in dict_.keys():
                dt = datetime.strptime(dict_['Last-Modified'], '%a, %d %b %Y %H:%M:%S %Z')
                dt_ = dt.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('US/Eastern'))
                now_ = now('US/Eastern')

                if (now_ - dt_).total_seconds()/60 < 120:  # not older than two hours
                    #logger.info(f'{camid} - Successfully captured image')
                    #print(f'{camid} - Successfully captured image')
                    return json_data
                else:
                    #logger.warning(f'{camid} - Image too old')
                    #print(f'{camid} - Image too old')
                   pass
        else:
            #logger.warning(f'{camid} - No data')
            #print(f'{camid} - No data')
            pass

    except Exception as e:
        #logger.error(f'{camid} - {str(e)}')
        #print(f'{camid} - {str(e)}')
        pass



if __name__ == '__main__':

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    hmsf = now('US/Eastern').strftime('%H%M%S%f')
    camids = get_camids_nav()

    logger.info(f'{len(camids)} cameras to ping')

    with Pool(24) as pool:
        results = pool.map_async(get_camera_data, camids)
        pool.close()
        pool.join()
    data = results.get()
    jsons = [d for d in data if d is not None]

    json_string = '[{}]'.format(','.join(jsons))
    df = pd.read_json(json_string).rename(columns={'Date': 'Datetime'})

    df['LMD'] = pd.to_datetime(df['Last-Modified'], format='%a, %d %b %Y %H:%M:%S %Z')
    df['LMD'] = df.LMD.dt.tz_convert('US/Eastern')
    df['LMD'] = df.LMD.dt.strftime('%Y-%m-%d')

    # separate output file for each Last Modified day. For when spanning multiple days
    for yyyy_mm_dd in df.LMD.drop_duplicates().values:
        parquet_key = f'mark/cctvlogs/date={yyyy_mm_dd}/cctvlog_{yyyy_mm_dd}_{hmsf}.parquet'

        return_df = df[df.LMD == yyyy_mm_dd].drop(columns=['LMD'])
        s3io.write_parquet(df, Bucket=conf['bucket'], Key=parquet_key)
        logger.success(f'Wrote {conf["bucket"]}/{parquet_key}')
