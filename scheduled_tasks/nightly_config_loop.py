# -*- coding: utf-8 -*-
"""
Created on Fri May  8 12:01:42 2020

@author: V0010894
"""

import sys
import pandas as pd
import datetime

from nightly_config import *

    
if __name__=='__main__':

    if len(sys.argv)==3:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    
    elif len(sys.argv)==2:
        start_date = sys.argv[1]
        end_date = sys.argv[1]


    else:
        sys.exit("Need one or two command line arguments")    

    """
    # Code to go back and calculate past days
    date_string = '2019-10-20'
    mvdp = reduce_det_plans()
    ad = get_atspm_detectors()
    det_config = get_det_config(ad, mvdp, date_string)
    
    
    dates = pd.date_range('2020-05-01', '2020-05-01', freq='1D')
    """
    rms = reduce_rms_detectors()
    
    
    dates = pd.date_range(start_date, end_date, freq='1D')
    
    for date_ in dates:
        date_string = date_.strftime('%Y-%m-%d')
        print(date_string)
        
        # ad = get_atspm_detectors(date_string)
        ad = pd.read_csv(f's3://gdot-devices/atspm_det_plans/date={date_string}/ATSPM_Det_Plans.csv')
        mvdp = pd.read_csv(f's3://gdot-devices/maxtime_det_plans/date={date_string}/MaxTime_Det_Plans.csv')
        
        print('{} Getting ATSPM Detector Config (Good): Merge of MaxTime and ATSPM...'.format(
            datetime.now().strftime('%x %X')))
    
        ad = ad[~ad.SignalID.isin(rms.SignalID.values)]
        rms_ad = (rms.rename(columns={'Location': 'PrimaryName', 
                                      'IP': 'IPAddress', 
                                      'Detector Type': 'DetectionTypeDesc', 
                                      'Lane': 'LaneNumber'}))
        mvdp = mvdp[~mvdp.SignalID.isin(rms.SignalID.values)]
        rms_mvdp = (rms.rename(columns={'Location': 'Name'})
                    .drop(columns=['Detector Type', 'Lane'])
                    .assign(CallPhase = lambda x: x.CallPhase.astype('int'))
                    .filter(['SignalID', 'IP', 'Name', 'Detector', 'CallPhase'], axis=1))
        
        ad = pd.concat([ad, rms_ad], sort=True)
        mvdp = pd.concat([mvdp, rms_mvdp], sort=True)
        det_config = get_det_config(ad, mvdp, date_string)
        dc_filename = 'ATSPM_Det_Config_Good_{}.feather'.format(
                date_string)
        det_config.to_feather(dc_filename)
        
        # upload to s3
        upload_to_s3(
                dc_filename, 
                'atspm_det_config_good/date={}/ATSPM_Det_Config_Good.feather', 
                date_)
