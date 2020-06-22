# -*- coding: utf-8 -*-
"""
Created on Thu Feb 27 12:48:44 2020

@author: V0010894
"""

import asyncio
from aiohttp import ClientSession #, ClientTimeout
import pandas as pd
import os
import sqlalchemy as sq
from bs4 import BeautifulSoup
import ipaddress

sem = asyncio.Semaphore(1000)

from get_intersections import get_intersections_list


def equal_octets(ip):
    if len(set(ip.split('.'))) == 1:
        return True
    else:
        return False


def is_valid_ip(ip):
    try:
        ipaddress.IPv4Network(ip)
        # Equal octets (e.g., 10.10.10.10) 
        # or all digits are the same (e.g., 0.00.000.0000 or 1.11.111.1111)
        if equal_octets(ip) == True or len(set(ip.replace('.',''))) == 1:
            print(f'{ip}: Flagged as likely a bad IP')
            return False
        else:
            return True
    except ipaddress.AddressValueError as e:
        print(f'{ip}: {e}')
        return False


def get_maxtime_ips():
    
    uid = os.environ['MAXV_USERNAME']
    pwd = os.environ['MAXV_PASSWORD']
    engine_maxv = sq.create_engine(
            'mssql+pyodbc://{}:{}@MaxView'.format(uid, pwd), pool_size=20)
    uid = os.environ['ATSPM_USERNAME']
    pwd = os.environ['ATSPM_PASSWORD']
    engine_atspm = sq.create_engine(
            'mssql+pyodbc://{}:{}@atspm'.format(uid, pwd), pool_size=20)
        
    ints = get_intersections_list(engine_maxv, engine_atspm, pd.to_datetime('1900-01-01'))
    ips = list(set(list(ints.IPAddress.dropna().values) + list(ints.HostAddress.dropna().values)))
    ips = [ip for ip in ips if not ip.startswith('1.')]
    ips = [ip for ip in ips if is_valid_ip(ip)]
    ips.sort()
    
    return ips


async def get_html(session, ip):
    try:
        response = await session.get(f'http://{ip}')
        html = await response.text()
        print(html[:100])
    except Exception as e:
        print(f'{ip}]: {e}')

    
async def get_headers(session, ip):
    try:
        response = await session.get(f'http://{ip}')
        head = response.headers
        if 'Server' in head:
            server = head['Server']
        else:
            server = 'No Server in header'

        html = await response.text()
        soup = BeautifulSoup(html, 'lxml')
        if soup.find('title'):
            title = soup.find('title').text
        else:
            title = None

        print('.', end='')
        return {'IP': ip, 'Server': server, 'Title': title}
    
    except Exception as e:
        print(f'\n{ip}]: {e}')
        return {'IP': ip, 'Server': 'Cannot connect to host', 'Title': None}


async def main():
    
    ips = get_maxtime_ips()
    
    #with open('ips_temp.txt') as f:
    #    ips = [x.strip() for x in f.readlines()]
    
    # work with shared resource
    async with sem:
        async with ClientSession() as session: #timeout=timeout
            tasks = [asyncio.create_task(get_headers(session, ip)) for ip in ips]
            #for task in tasks:
            #    await task
            results = [await task for task in tasks]
    return pd.DataFrame(results)

df = asyncio.run(main())
#df = await main()

df[df.Server=='nginx/1.10.3'].to_csv('maxtime2_ips.csv')

df_mt1 = df[df.Title=='MaxTime Database Editor']
df_mt1.to_csv('maxtime1_ips.csv')
with open('maxtime1_ips.txt','w') as f:
    f.write('\n'.join(df_mt1.IP.values))
    
df_rm = df[df.Title=='RampMeter Database Editor']
df_rm.to_csv('rampmeter_ips.csv')
with open('rampmeter_ips.txt','w') as f:
    f.write('\n'.join(df_rm.IP.values))

df_noresp = df[df.Server=='Cannot connect to host']
df_noresp.to_csv('noresponse_ips.csv')
