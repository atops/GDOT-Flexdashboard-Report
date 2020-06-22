# -*- coding: utf-8 -*-
"""
Created on Fri Feb 28 15:31:46 2020

@author: V0010894
"""

import sys
import re
import os
from glob import glob
from datetime import datetime
import random
import pandas as pd
from multiprocessing import Pool
import itertools
import time
import numpy as np
import shutil

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
        NoSuchElementException,
        StaleElementReferenceException,
        TimeoutException)

sys.path.insert(1, 'scheduled_tasks')

ignored_exceptions = (NoSuchElementException, 
                      StaleElementReferenceException,)

def extract_detector_plan_table(driver, wait, ip):
    wait.until(EC.visibility_of_any_elements_located((By.CSS_SELECTOR, 'span')))
    # Get Table Tab
    #print(driver.title)
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'div:nth-child(2) > button > div > span')))
    elem = driver.find_element_by_css_selector('div > div > div:nth-child(3) > div > div > div:nth-child(1) > div:nth-child(2) > button > div > span')
    elem.click()
    #-- ---------------------------------- -- --------------------------
    # We can't just get the body because of the javascript. 
    # We need to go all the way and parse the page.
    wait.until(EC.visibility_of_any_elements_located((By.CLASS_NAME, 'ReactVirtualized__Grid__innerScrollContainer')))
    elems = driver.find_elements_by_class_name('ReactVirtualized__Grid__innerScrollContainer')
    
    det_header = [x.text for x in elems[0].find_elements_by_xpath('div')]
    detectors = [x.text for x in elems[1].find_elements_by_xpath('div')]
    header = [x.text for x in elems[2].find_elements_by_xpath('div')]
    values = [x.text for x in elems[3].find_elements_by_xpath('div')]
    
    mat = np.array_split(values, len(values)/len(header))
    df = pd.DataFrame(mat, columns=header)
    df.insert(0, det_header[0], detectors)
    df.insert(0, 'IP', ip)
    
    return df


def get_det_config_maxtime_new_ui(ips):
    #10.251.217.6
    # Open Chrome window with options (to avoid flag error)
    opts = webdriver.ChromeOptions()
    opts.add_argument("--test-type")
    opts.add_argument("--headless")
    opts.add_argument("--window-size=4840x2160") 
    opts.add_argument('log-level=3')
    
    #driver = webdriver.Chrome(options=opts)
    with webdriver.Chrome(options=opts) as driver:
    
        # Set timeout time to 10 seconds
        wait = WebDriverWait(driver, 10, ignored_exceptions=ignored_exceptions)
    
        def get_table(driver, ip):
            
            dfv0 = pd.DataFrame(
                    {'IP': [ip],
                     'Name': np.nan,
                     'Detector': np.nan,
                     'Description': np.nan, 
                     'Call Phase': np.nan, 
                     'Call Overlap': np.nan, 
                     'Call Ped': np.nan, 
                     'Additional Call Phases': np.nan, 
                     'Switch Phase': np.nan,
                     'Delay': np.nan, 
                     'Extend': np.nan, 
                     'Queue Limit': np.nan,
                     'Extension Hold': np.nan,
                     'No Activity': np.nan,
                     'Max Presence': np.nan,
                     'Erratic Count': np.nan,
                     'Fail Time': np.nan, 
                     'Failed Recall': np.nan,
                     'Failed Link': np.nan})
            dfp0 = pd.DataFrame(
                    {'IP': [ip],
                     'Name': np.nan,
                     'Detector': np.nan, 
                     'Call Phase': np.nan, 
                     'Call Overlap': np.nan, 
                     'Additional Call Phases': np.nan, 
                     'Walk 2 Enable Time': np.nan, 
                     'Ped Clear 2 Enable Time': np.nan, 
                     'No Activity': np.nan, 
                     'Max Presence': np.nan, 
                     'Erratic Count': np.nan})

            driver.get(f'http://{ip}/maxtime')
                
            if not 'rampmeter' in driver.title:
                
                try:
                    # Sign In as Guest
                    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'div:nth-child(2) > button'))) # 'button')))
                    elem = driver.find_element_by_css_selector('div:nth-child(2) > button')
                    elem.click()
                    
                    wait.until(EC.visibility_of_any_elements_located((By.CSS_SELECTOR, 'h1+div')))
                    signal_name = driver.find_elements_by_css_selector('h1+div')[0].text.strip()
                    div = driver.find_elements_by_tag_name('div')[0]
                    stuff = div.text.split('\n')
                    if not signal_name:
                        signal_name = stuff[stuff.index('LOCATION')+1]
                    print(f'{ip}: {signal_name}|{len(signal_name)}')
                
                except TimeoutException:
                    print(f'{ip}: No Guest Login')
                    dfv = dfv0.copy()
                    dfp = dfp0.copy()
                    
                else:
                    if 'Controller' in stuff: # in menu
                        # Navigate to the vehicle detector page
                        try:
                            driver.get('http://{}/maxtime/Controller/Detector/vehicleDetectors'.format(ip))
                            dfv = extract_detector_plan_table(driver, wait, ip) 
                            dfv.insert(1, 'Name', signal_name)
                            dfv = pd.concat([dfv, dfv0], sort=True)
                            #dfv.to_parquet(f'../det_plan_maxtime2/{ip.replace(".","_")}.parquet')
                            dfv.to_csv(f'../det_plan_maxtime2/{ip.replace(".","_")}.csv')
                        except Exception as e:
                            print(f'{ip}: {e} - Error getting vehicle detectors')
                            dfv = dfv0.copy()
                            #dfv.to_parquet(f'../det_plan_maxtime2/{ip.replace(".","_")}.parquet')
                            dfv.to_csv(f'../det_plan_maxtime2/{ip.replace(".","_")}.csv')
                            
                        # Navigate to the pedestrian detector page
                        try:
                            driver.get('http://{}/maxtime/Controller/Detector/pedDetectors'.format(ip))
                            dfp = extract_detector_plan_table(driver, wait, ip)
                            dfp.insert(1, 'Name', signal_name)
                            #dfp.to_parquet(f'../ped_plan_maxtime2/{ip.replace(".","_")}.parquet')
                            dfp.to_csv(f'../ped_plan_maxtime2/{ip.replace(".","_")}.csv')
                        except Exception as e:
                            print(f'{ip}: {e} - Error getting ped detectors')
                            dfp = dfp0.copy()
                    else:
                        print(f'{ip}: Controller not in menu.')
                        dfv = dfv0.copy()
                        dfp = dfp0.copy()
            else: # rampmeter in title
                print(f'{ip}: Ramp meter controller')
                dfv = dfv0.copy()
                dfp = dfp0.copy()
                    
            print(f'{ip}: completed')
            return dfv, dfp

            
        x = [get_table(driver, ip) for ip in ips]
        
    return x
    

def remove_maxtime1_fns():
    
    df = pd.read_csv('maxtime2_ips.csv')
    ips = list(df.IP.values)
    ips_ = [ip.replace(".", "_") for ip in ips]
    for ip_ in ips_:
        maxtime1_fns = glob(f'../unit_info/{ip_}_*')
        if maxtime1_fns:
            print(ip_)
            print(maxtime1_fns)
            print('---')
            for mt1fn in maxtime1_fns:
                os.remove(mt1fn)
    '''
    for fn in os.listdir('../det_plan_maxtime2'):
        ip = re.search('\d+_\d+_\d+_\d', fn).group()
        maxtime1_fns = glob(f'../det_plan_html/{ip}_*')
        random.shuffle(maxtime1_fns)
        if maxtime1_fns:
            print(ip)
            print(maxtime1_fns)
            print('---')
            for mt1fn in maxtime1_fns:
                os.remove(mt1fn)
    '''

def get_maxtime1_ips():
    
    def get_maxtime1_fns():
        for fn in os.listdir('../det_plan_html'):
            ip = re.search('\d+_\d+_\d+_\d+', fn).group()
            yield ip
    
    ips = list(set(get_maxtime1_fns()))
    return ips

def get_fresh_maxtime2_ips():
    
    def get_maxtime2_fns():
        for fn in glob('../det_plan_maxtime2/*.csv'):
            last_modified = pd.Timestamp(os.stat(fn).st_mtime*1e9)
            if datetime.today() - last_modified < pd.Timedelta(48, unit = 'h'):
                ip = re.search('\d+_\d+_\d+_\d+', fn).group().replace('_', '.')
                yield ip
    ips = list(set(get_maxtime2_fns()))
    return ips

if __name__=='__main__':

    t0 = time.time()

    df = pd.read_csv('maxtime2_ips.csv')
    maxtime2ips = df.IP.values
    maxtime2ips = list(set(maxtime2ips) - set(get_fresh_maxtime2_ips()))
    random.shuffle(maxtime2ips)
    print(len(maxtime2ips))
    
    # temporary
    #ped_ips = [re.search('\d+_\d+_\d+_\d+', fn).group().replace('_','.') for fn in glob('../ped_plan_maxtime2/*.parquet')]
    #maxtime2ips = list(set(maxtime2ips) - set(ped_ips))
    #maxtime2ips = maxtime2ips[:16]
    
    ip_list_of_lists = np.array_split(maxtime2ips, 8)
    
    with Pool(8) as pool:
        results = pool.map_async(get_det_config_maxtime_new_ui, ip_list_of_lists)
        pool.close()
        pool.join()
    
    llt = results.get()  # list of list of tuples
    #lt = itertools.chain(*llt)  # list of tuples
    lt = list(itertools.chain(*llt))  # list of tuples
    lt = [x for x in lt if x]
    
    dfs_lists = list(zip(*lt))  # unzip into two lists
    dfv = pd.concat(dfs_lists[0], sort=True)  # first list is dfv's
    dfp = pd.concat(dfs_lists[1], sort=True)  # second list is dfp's
    
    first_cols = ['Name', 'IP', 'Detector', 'Call Phase']
    
    dfv = dfv[~dfv.Detector.isna()]
    other_cols = list(set(dfv.columns.to_list()) - set(first_cols))
    dfv = dfv[first_cols + other_cols]
    dfv.to_csv('maxtime2_det_plans.csv')
    
    dfp = dfp[~dfp.Detector.isna()]
    other_cols = list(set(dfp.columns.to_list()) - set(first_cols))
    dfp = dfp[first_cols + other_cols]
    dfp.to_csv('maxtime2_ped_plans.csv')
    
    print('--')
    print(round((time.time()-t0)/60, 1))
    
    # Remove temporary files created by chromedriver
    tmpdir = 'C:/Users/V0010894/AppData/Local/Temp'
    files = os.listdir(tmpdir)
    for f in files:
        try:
            shutil.rmtree(os.path.join(tmpdir, f))
            print(f"Removed {f}")
        except:
            print(f"Can't remove {f}")
