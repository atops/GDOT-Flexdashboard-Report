# -*- coding: utf-8 -*-
"""
Created on Wed Jul 31 15:35:13 2019

@author: V0010894
"""

import cv2

#import sys
import time
from datetime import datetime
import re
import csv
import pandas as pd
from multiprocessing import Pool
#from multiprocessing.dummy import Pool
from dask.distributed import Client
import requests
from bs4 import BeautifulSoup

    
#uri = 'rtsp://vssod.dot.ga.gov:80/hi/alph-cam-003.stream'



def get_img_from_id(camid):
    
    rtsp_uri = 'rtsp://vss1live.dot.ga.gov:80/lo/{}.stream' # camid (lowercase)
    
    stream = cv2.VideoCapture(rtsp_uri.format(camid).lower())
    valid, frame1 = stream.read()

    if not valid:
        print(camid, valid, None)
    else:
        print(camid, valid, frame1.shape, frame1.sum())

def get_camera_type(camera):
    
    # This function gets the title of the window after 
    # following the link to the video stream

    uri = 'http://{}'.format(camera['Host/IP']) # IP
    try:
        response = requests.get(uri, timeout=5)
        content = response.content
        c = content.decode('utf-8')
        soup = BeautifulSoup(c, 'html.parser')
        meta_contents = '|'.join(['|'.join(x.values()) for x in [meta.attrs for meta in soup.find_all('meta')]]).replace(',',' ')
        title_search = re.search('<title>(.*?)</title>', c, re.IGNORECASE)
        if title_search:
            title = title_search.groups()[0]
        else:
            title = ''
            
        if title=='Index page' and re.search('/view/viewer_index.*', meta_contents):
            uri_suffix = re.search('/view/viewer_index.*', meta_contents).group()
            uri = uri + uri_suffix
            response = requests.get(uri, timeout=5)
            content = response.content
            c = content.decode('utf-8')
            soup = BeautifulSoup(c, 'html.parser')
            meta_contents = '|'.join(['|'.join(x.values()) for x in [meta.attrs for meta in soup.find_all('meta')]]).replace(',',' ')
            title_search = re.search('<title>(.*?)</title>', c, re.IGNORECASE)
            if title_search:
                title = title_search.groups()[0]
            else:
                title = ''
            
        return title
    except:
        return ''
    

def get_img(camera):
    
    camera['Title'] = get_camera_type(camera)
    
    # This function tries a bunch of different connection strings
    # to find one that works. We don't really know a priori what 
    # the camera type is, and therefore what the connection string
    # should be.
    
    camip = camera['Host/IP']
    camid = camera['Location ID']
    
    # First check for comms
    try:
        response = requests.get('http://{}'.format(camip), timeout=5)
        #if response.status_code != 200: raise Exception

    
        #rtsp_uri = 'rtsp://vss1live.dot.ga.gov:80/lo/{}.stream' # camid (lowercase)
        mjpg_uri = 'http://{}/mjpg/video.mjpg' # IP
        #jpeg_uri = 'http://{}/cgi-bin/jpg/image.cgi' # IP
        
        
        # Axis (Most common)
        uri = mjpg_uri.format(camip).lower()
        stream = cv2.VideoCapture(uri)
        if stream.isOpened():
            valid, frame1 = stream.read()
            connection_type = 1
        else:
            valid, frame1 = (False, None)
    
        # Siqura, Cohu
        if not valid:
            uri = 'rtsp://{}/h264'.format(camip)
            stream = cv2.VideoCapture(uri)
            if stream.isOpened():
                valid, frame1 = stream.read()
                connection_type = 2
            else:
                valid, frame1 = (False, None)
    
        # Pelco
        if not valid:
            #uri = rtsp_uri.format(camid).lower()
            uri = 'rtsp://{}/VideoInput/1/h264/1'.format(camip)
            stream = cv2.VideoCapture(uri)
            if stream.isOpened():
                valid, frame1 = stream.read()
                connection_type = 3
            else:
                valid, frame1 = (False, None)
    
    
        # Cohu
        if not valid:
            uri = "rtsp://{}/stream1"
            stream = cv2.VideoCapture(uri.format(camip))
            if stream.isOpened():
                valid, frame1 = stream.read()
                connection_type = 4
            else:
                valid, frame1 = (False, None)
    
        # Axis (rare)
        if not valid:
            uri = mjpg_uri.format('RTOP:Tr%40ff1c0ps@'+camip)
            stream = cv2.VideoCapture(uri)
            if stream.isOpened():
                valid, frame1 = stream.read()
                connection_type = 5
            else:
                valid, frame1 = (False, None)
        
        # Siqura PD1102
        if not valid:
            uri = 'rtsp://User:Tran$portation@{}:554'.format(camip)
            stream = cv2.VideoCapture(uri)
            if stream.isOpened():
                valid, frame1 = stream.read()
                connection_type = 6
    
        if not valid:
            uri = "rtsp://{}/channel1"
            stream = cv2.VideoCapture(uri.format(camip))
            if stream.isOpened():
                valid, frame1 = stream.read()
                connection_type = 7
            else:
                valid, frame1 = (False, None)
    
        # Axis
        if not valid:
            uri = 'rtsp://{}/axis-media/media.amp'
            stream = cv2.VideoCapture(uri.format(camip))
            if stream.isOpened():
                valid, frame1 = stream.read()
                connection_type = 8
            else:
                valid, frame1 = (False, None)
                    
        if not valid:
            camera['Datetime'] = datetime.now().strftime('%Y-%m-%d %H:%H:%S')
            camera['Response'] = str(valid)
            camera['Dimensions'] = ''
            camera['Size'] = 0
            camera['Connection Type'] = 0
        
            return camera
            
        else: # valid
            if type(frame1)==str:
                dims = '0x0'
                size = frame1
            else:
                dims = '{}x{}'.format(frame1.shape[1], frame1.shape[0])
                size = frame1.sum()
                
            camera['Datetime'] = datetime.now().strftime('%Y-%m-%d %H:%H:%S')
            camera['Response'] = str(valid)
            camera['Dimensions'] = dims
            camera['Size'] = size
            camera['Connection Type'] = connection_type
            
            return camera
            
            #        print(','.join([camid, 
            #                        camip, 
            #                        camera['Manufacturer'], 
            #                        str(valid), 
            #                        dims, 
            #                        size, 
            #                        str(connection_type)]))

    except:
        print(','.join([camid, 
                        camip, 
                        camera['Manufacturer'], 
                        "No response"]))
    
        camera['Datetime'] = datetime.now().strftime('%Y-%m-%d %H:%H:%S')
        camera['Response'] = 'None'
        camera['Dimensions'] = ''
        camera['Size'] = 0
        camera['Connection Type'] = 0
        
        return camera

if __name__=='__main__':
    
    t0 = datetime.now()
    print(t0.strftime('%x %X'))

    #    with open('c:/Users/V0010894/Code/GDOT/cameras_list.csv') as f:
    #        reader = csv.DictReader(f)
    #        cameras = [r for r in reader if r['Host/IP']!=''][:10]
    
    cameras = pd.read_excel('Camera_Encoders_Latest.xlsx')\
                .query('Include==True')\
                .to_dict(orient='records')

    #cameras = cameras[:80]

    print('{} cameras to view.'.format(len(cameras)))

    #with Pool() as pool:
    with Pool(processes=16) as pool:
        results = pool.map_async(get_img, cameras)
        pool.close()
        pool.join()
    
    data = results.get()
    for datum in data:
        print(datum)
    df = pd.DataFrame(data)
    
    date_string = t0.date().strftime('%Y-%m-%d')
    hmsf = datetime.now().strftime('%H%M%S%f')
    key_suffix = 'date={d}/cctv_{d}_{t}.parquet'.format(d=date_string, t=hmsf)
    
    df.to_parquet('s3://gdot-spm/mark/cctvlogs_encoder/{}'.format(key_suffix))
    
    t1 = datetime.now()
    print(t1.strftime('%x %X'))
    print(len(cameras), 'in', round((t1-t0).total_seconds()/60, 1), 'minutes')
    