# -*- coding: utf-8 -*-
"""
Created on Wed Jan 16 15:28:33 2019

@author: alan.toppen
"""
import requests
import os
import yaml
from datetime import datetime
import json
import pytz


def get_camids(camids_file):
    with open(camids_file) as f:
        for camid in f.readlines()[1:]:
            yield camid.split(': ')[0].strip()
            

def get_camera_data(camid):
    response = requests.get('http://cdn.511ga.org/cameras/{}.jpg'.format(camid))
    print(camid, end=': ')

    if response.status_code == 200:

        dict_ = dict(response.headers)
        dict_['ID'] = camid
        json_data = json.dumps(dict_)
        
        if 'Last-Modified' in dict_.keys():
            dt = datetime.strptime(dict_['Last-Modified'], '%a, %d %b %Y %H:%M:%S %Z')
            dt_ = dt.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('US/Eastern'))
            now_ = datetime.now().astimezone(pytz.timezone('US/Eastern'))
            
            if (now_ - dt_).total_seconds()/60 < 120: # not older than two hours
                print(json_data)
                fn = os.path.join(base_path, 'cctvlog_{}.json'.format(dt_.strftime('%Y-%m-%d')))
                
                with open(fn, mode='a') as f:
                    f.write(json_data + '\n')
            else:
                print('Data too old.')

    else:
        print('No data.')

    print('---')
    


if __name__=='__main__':

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)
    
    gdot_folder = os.path.join(os.path.expanduser('~'), 'Code', 'GDOT', 'GDOT-Flexdashboard-Report')
    camids_file = os.path.join(gdot_folder, conf['cctv_config_filename'])
    
    base_path = os.path.join(gdot_folder, '../cctvlogs')
        
    for camid in get_camids(camids_file):
        get_camera_data(camid)
