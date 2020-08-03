# -*- coding: utf-8 -*-
"""
get_maxtime_asyncio_polling#.py

Created on Fri Jun 23 08:08:49 2017

@author: Alan.Toppen
"""

# modified fetch function with semaphore
import pandas as pd
import sqlalchemy as sq
import asyncio
from aiohttp import ClientSession, ClientTimeout
import os
import polling
from urllib.parse import urlparse
import re
import pyodbc
from datetime import datetime
import random

from get_intersections import get_intersections_list

sem = asyncio.Semaphore(1000)


async def write_events_to_file(url, html, file_suffix, base_path):
    
    ip = urlparse(url).netloc        
    filename = '{}{}'.format(ip.replace(".","_").replace("/","_"), file_suffix)
    filepath = os.path.join(base_path, filename)
    
    #if not os.path.exists(filepath): # POTENTIALLY TEMPORARY. EVALUATE LATER
    #print (filepath)
    
    with open(filepath, 'w') as f:
        f.write(html)
    
    
async def fetch(url, menu_entry, file_suffix, base_path, session): #out_file, 
    try:
        response = (await polling.poll(lambda: session.get(url), 
                                       #ignore_exceptions=(ClientConnectionError, 
                                       #                   ClientConnectorError), 
                                       step=1,
                                       timeout=5))
        
        html = await response.text()
        response_received = True

    except Exception:
        print('{} - Error: Problem getting response'.format(urlparse(url).netloc))
        response_received = False

    if response_received:
            
        try:
            pattern = '{}\', \'(generateForm.cgi\?formID=\d+)\''.format(menu_entry)
            plan_exts = re.findall(pattern, html)
            
            if menu_entry in ['Demand Detectors', 'Passage Detectors', 'Metered Queues']:
                i = 1
            else:
                i = 0
            
            plan_ext = plan_exts[i] #[i]
            
            if menu_entry in ['Vehicle Detector Plans', 'Pedestrian Detector Plans']:
                
                for p in range(4): # Four pages
                    url2 = 'http://{}/cgi-bin/{}{}'.format(urlparse(url).netloc, 
                            plan_ext,
                            '&form0Page=0&form1Page={}&form1Index0=1&saveChanges=false'.format(p))
        
                    response2 = (await polling.poll(lambda: session.get(url2), 
                                                    step=0.2,
                                                    timeout=1))
                    html = await response2.text()
                    
                
                    if 'Call Phase' in html:
                                                   
                        # This is where the magic happens -----
                        await write_events_to_file(url2, html, '{}'.format(file_suffix.replace('.', '_' + str(p) + '.')), base_path)
                        # -------------------------------------
            
            elif menu_entry == 'Metered Queues':
                for p in range(1,5):  # [1,2,3,4]
                           
                    url2 = 'http://{}/cgi-bin/{}{}'.format(urlparse(url).netloc, 
                            plan_ext,
                           '&form0Page=0&form0Index0={p}&form1Page=0&form1Index0=1'.format(p=p))
    
                    response2 = (await polling.poll(lambda: session.get(url2), 
                                                    step=0.2,
                                                    timeout=1))
                    html = await response2.text()
                    # This is where the magic happens -----
                    await write_events_to_file(url2, html, '{}'.format(file_suffix.replace('.', '_' + str(p) + '.')), base_path)
                    # -------------------------------------
                
            else:
                url2 = 'http://{}/cgi-bin/{}'.format(urlparse(url).netloc, plan_ext)
    
                response2 = (await polling.poll(lambda: session.get(url2), 
                                                step=0.2,
                                                timeout=1))
                html = await response2.text()
    
                await write_events_to_file(url2, html, '{}'.format(file_suffix), base_path)
            
            print('{} - Success'.format(urlparse(url).netloc))
            return await response.release()
    
        except Exception as e:
            print('{} - Error Parsing: {}'.format(urlparse(url).netloc, e))


async def run(maxtime1ips, menu_entry, file_suffix, base_path):
    url = "http://{}{}"
    url_suffix='/cgi-bin/generateForm.cgi?formID=2004'
    #url_suffix=''

    # create instance of Semaphore
    #timeout = ClientTimeout(total=5)
    
    #async with sem:
    async with ClientSession() as session: 
        tasks = [asyncio.create_task(
                    fetch(url.format(ip.strip(), url_suffix), 
                          menu_entry,
                          file_suffix, 
                          base_path, 
                          session))
            for ip in maxtime1ips]

        results = [await task for task in tasks]
    #return results


def get_maxtime_data(menu_entry, path_name, file_suffix, ips_file=None):

    if not ips_file:
        if os.name == 'nt':
            
            uid = os.environ['MAXV_USERNAME']
            pwd = os.environ['MAXV_PASSWORD']
            engine_maxv = sq.create_engine('mssql+pyodbc://{}:{}@MaxView'.format(uid, pwd), 
                                            pool_size=20)
            #engine_mvel = sq.create_engine('mssql+pyodbc://{}:{}@MaxView_EventLog', pool_size=20)
            uid = os.environ['ATSPM_USERNAME']
            pwd = os.environ['ATSPM_PASSWORD']
            engine_atspm = sq.create_engine('mssql+pyodbc://{}:{}@atspm'.format(uid, pwd), 
                                            pool_size=20)
        elif os.name == 'posix':
            
            def connect():
                return pyodbc.connect(
                    'DRIVER=FreeTDS;' + 
                    'SERVER={};'.format(os.environ['MAXV_SERVER_INSTANCE']) +
                    'DATABASE={};'.format(os.environ['MAXV_DB']) +
                    'UID={};'.format(os.environ['MAXV_USERNAME']) +
                    'PWD={};'.format(os.environ['MAXV_PASSWORD']) +
                    'TDS_Version=8.0;')
            
            engine_maxv = sq.create_engine('mssql://', creator=connect)
            
            def connect():
                return pyodbc.connect(
                    'DRIVER=FreeTDS;' + 
                    'SERVER={};'.format(os.environ["ATSPM_SERVER_INSTANCE"]) +
                    'DATABASE={};'.format(os.environ["ATSPM_DB"]) +
                    'UID={};'.format(os.environ['ATSPM_USERNAME']) +
                    'PWD={};'.format(os.environ['ATSPM_PASSWORD']) +
                    'TDS_Version=8.0;')
            
            engine_atspm = sq.create_engine('mssql://', creator=connect)
        
        ints = get_intersections_list(engine_maxv, engine_atspm, datetime.today()) # pd.to_datetime('1900-01-01'))
        ips = list(set(list(ints.IPAddress.dropna().values) + list(ints.HostAddress.dropna().values)))
        ips = [ip for ip in ips if re.match('^\d{2,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', ip)]
        ips.sort()
        
        ips_file = 'ips_temp.txt'
        with open(ips_file, 'w') as f:
            f.write('\n'.join(ips))
    
    else: 
        with open(ips_file, 'r') as f:
            ips = [line.strip() for line in f.readlines()]
    print(len(ips))
    random.shuffle(ips)

    base_path = os.path.expanduser('~')
    base_path = os.path.join(base_path, 'Code', 'GDOT', path_name)

    asyncio.run(run(ips, menu_entry, file_suffix, base_path))



def get_veh_det_plans():
    get_maxtime_data(menu_entry='Vehicle Detector Plans',
                     file_suffix = '_det_plans.html',
                     path_name='det_plan_html',
                     ips_file='maxtime1_ips.txt')



def get_ped_det_plans():
    get_maxtime_data(menu_entry='Pedestrian Detector Plans',
                     file_suffix = '_ped_plans.html',
                     path_name='ped_plan_html',
                     ips_file='maxtime1_ips.txt')



def get_unit_info():
    get_maxtime_data(menu_entry='Unit Information',
                     file_suffix = '_unit_info.html',
                     path_name='unit_info',
                     ips_file='maxtime1_ips.txt')


def get_preempt_phasing():
    get_maxtime_data(menu_entry='Preempt Phasing',
                     file_suffix='_preempt_phasing.html',
                     path_name='preempt_phasing',
                     ips_file='maxtime1_ips.txt')
    
    
def get_rms_mainline_parameters():
    get_maxtime_data(menu_entry='Mainline Parameters',
                     file_suffix='_rms_ml_params.html',
                     path_name='ramp_meters/mainline_parameters',
                     ips_file='ips_rms.txt')


def get_rms_demand_detectors():
    get_maxtime_data(menu_entry='Demand Detectors',
                     file_suffix='_rms_demand_detectors.html',
                     path_name='ramp_meters/demand_detectors',
                     ips_file='ips_rms.txt')
    
    
def get_rms_passage_detectors():
    get_maxtime_data(menu_entry='Passage Detectors',
                     file_suffix='_rms_passage_detectors.html',
                     path_name='ramp_meters/passage_detectors',
                     ips_file='ips_rms.txt')


def get_rms_queue_detectors():
    get_maxtime_data(menu_entry='Metered Queues',
                     file_suffix='_rms_metered_queues.html',
                     path_name='ramp_meters/metered_queues',
                     ips_file='ips_rms.txt')


if __name__=='__main__':

    #get_unit_info()
    #get_veh_det_plans()
    get_ped_det_plans()
    #get_preempt_phasing()
    #get_rms_mainline_parameters()
    """



    file_suffix = "_intersection.html"
    url_suffix = "/cgi-bin/generateForm.cgi?formID=115&amp;form0Page=0&amp;form1Page=0&amp;saveChanges=true"
   
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(ips_file, url_suffix, file_suffix, os.path.join(base_path,'115')))
    loop.run_until_complete(future)
    
    url_suffix = "/cgi-bin/generateForm.cgi?formID=119&amp;form0Page=0&amp;form1Page=0&amp;saveChanges=true"
    
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(ips_file, url_suffix, file_suffix, os.path.join(base_path,'119')))
    loop.run_until_complete(future)

    url_suffix = "/cgi-bin/generateForm.cgi?formID=120&amp;form0Page=0&amp;form1Page=0&amp;saveChanges=true"

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(ips_file, url_suffix, file_suffix, os.path.join(base_path,'120')))
    loop.run_until_complete(future)
    
    
    
    file_suffix = '_time.html'
    url_suffix = '/cgi-bin/generateForm.cgi?formID=131'

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(ips_file, url_suffix, file_suffix, base_path))
    loop.run_until_complete(future)
    
    file_suffix = '_tz.html'
    url_suffix = '/cgi-bin/generateForm.cgi?formID=132'

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(ips_file, url_suffix, file_suffix, base_path))
    loop.run_until_complete(future)


    file_suffix = '_time_source.html'
    url_suffix = '/cgi-bin/generateForm.cgi?formID=133'

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(ips_file, url_suffix, file_suffix, base_path))
    loop.run_until_complete(future)
    

    file_suffix = '_params.html'
    url_suffix = '/cgi-bin/generateForm.cgi?formID=49'

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(ips_file, url_suffix, file_suffix, base_path))
    loop.run_until_complete(future)
    """