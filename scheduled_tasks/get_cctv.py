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


def get_camids_s3():
    cameras_latest = 's3://gdot-spm/Cameras_Latest.xlsx'
    camids = list(pd.read_excel(cameras_latest).CameraID.values)
    return camids


def get_camids_nav():
    response = requests.get('http://navigator-c2c.dot.ga.gov/snapshots/')
    camids = sorted(list(set(
            re.findall('[^> /]*?-.*?-.*?(?=.jpg)', response.text)
        )))
    return camids


def get_camera_data(camid):
    try:
        if 'GWIN-CAM' in camid or 'GCDOT-CAM' in camid:
            url = 'http://dotatis.gwinnettcounty.com/atis/snapshots/{}.jpg'.format(
                camid.replace('GWIN', 'GCDOT'))
        else:
            url = 'http://cdn.511ga.org/cameras/{}.jpg'.format(camid)
        response = poll(lambda: requests.get(url, timeout=2), step=2, timeout=10,
                        ignore_exceptions=(requests.exceptions.ConnectionError))
        print(camid, end=': ')

        if response.status_code == 200:

            dict_ = dict(response.headers)
            dict_['ID'] = camid
            json_data = json.dumps(dict_)

            if 'Last-Modified' in dict_.keys():
                dt = datetime.strptime(dict_['Last-Modified'], '%a, %d %b %Y %H:%M:%S %Z')
                dt_ = dt.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('US/Eastern'))
                now_ = datetime.now().astimezone(pytz.timezone('US/Eastern'))

                if (now_ - dt_).total_seconds()/60 < 120:  # not older than two hours
                    print('Good')
                    return json_data
                else:
                    print('Data too old.')
        else:
            print('No data.')

    except Exception as e:
        print('{}: ERROR '.format(camid), e)



if __name__ == '__main__':

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    gdot_folder = os.path.join(
        os.path.expanduser('~'), 'Code', 'GDOT', 'GDOT-Flexdashboard-Report')
    camids_file = os.path.join(gdot_folder, conf['cctv_config_filename'])

    base_path = os.path.join(gdot_folder, '../cctvlogs')

    #s3.download_file(
    #    Bucket='gdot-spm', Key='navigator_camids.csv', Filename=camids_file)

    hmsf = datetime.today().strftime('%H%M%S%f')

    #camids1 = get_camids_xl(camids_file)
    camids2 = get_camids_s3()
    #camids2 = []
    camids3 = get_camids_nav()
    camids = list(set(camids2 + camids3))

    print('{} cameras.'.format(len(camids)))

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
        parquet_fn = os.path.join(base_path, 'cctvlog_{}_{}.parquet'.format(
            yyyy_mm_dd, hmsf))
        (df[df.LMD == yyyy_mm_dd]
            .drop(columns=['LMD'])
            .to_parquet('s3://gdot-spm/mark/cctvlogs/date={}/{}'.format(
                    yyyy_mm_dd, os.path.basename(parquet_fn))))
