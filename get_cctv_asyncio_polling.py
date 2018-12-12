# -*- coding: utf-8 -*-
"""
get_maxtime_asyncio_polling#.py

Created on Fri Jun 23 08:08:49 2017

@author: Alan.Toppen
"""

# modified fetch function with semaphore
import asyncio
from aiohttp import ClientSession
import os
import polling
import json
from datetime import datetime
import pytz
import yaml

def get_camids(camids_file):
    with open(camids_file) as f:
        for camid in f.readlines()[1:]:
            yield camid.split(': ')[0].strip()


async def print_response_header(camid, base_path, response):
    dict_ = dict(response.headers)
    dict_['ID'] = camid
    json_data = json.dumps(dict_)
    print(json_data)
    print('---')
    
    if 'Last-Modified' in dict_.keys():
        dt = datetime.strptime(dict_['Last-Modified'], '%a, %d %b %Y %H:%M:%S %Z')
        dt_ = dt.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('US/Eastern'))
        fn = os.path.join(base_path, 'cctvlog_{}.json'.format(dt_.strftime('%Y-%m-%d')))
        
        with open(fn, 'a') as f:
            f.write(json_data + '\n')
    
    
async def fetch(camid, base_path, session): #out_file, 
    try:
        url = 'http://cdn.511ga.org/cameras/{}.jpg'.format(camid)
        
        response = (await polling.poll(lambda: session.get(url,
                                                           timeout=5,
                                                           proxy="http://GADOT\V0010894:11400Comm20191@gdot-enterprise:8080"), 
                                       step=0.5,
                                       timeout=3))
        
        #print (camid)
        await print_response_header(camid, base_path, response)
        
        #return await response.release()
    except Exception as e:
        print('{} - Error: {}'.format(camid, e))
        #logger.error('{}, Failed, {},'.format(urlparse(url).netloc, e))


async def bound_fetch(sem, 
                      camid, 
                      base_path, 
                      session):
    # Getter function with semaphore.
    async with sem:
        await fetch(camid, base_path, session)

async def run(camids_file, base_path):

    tasks = []
    # create instance of Semaphore
    sem = asyncio.Semaphore(20)
    # Create client session that will ensure we dont open new connection
    # per each request.
    async with ClientSession(read_timeout=5) as session:
        for camid in get_camids(camids_file):
            print (camid)
            # pass Semaphore and session to every GET request
            task = asyncio.ensure_future(bound_fetch(sem, 
                                                     camid, 
                                                     base_path, 
                                                     session))
            tasks.append(task)

        responses = asyncio.gather(*tasks)
        await responses

if __name__=='__main__':
    
    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file)

    gdot_folder = os.path.join(os.path.expanduser('~'), 'Code', 'GDOT', 'GDOT-Flexdashboard-Report')
    camids_file = os.path.join(gdot_folder, conf['cctv_config_filename'])
    #'c:/users/V0010894/Code/GDOT/camera_ids.txt'

    base_path = os.path.join(gdot_folder, '../cctvlogs')
    #'C:/Users/V0010894/Code/GDOT/cctvlogs'

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(camids_file, base_path))
    loop.run_until_complete(future)
