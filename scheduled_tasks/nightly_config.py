# -*- coding: utf-8 -*-
"""
Created on Thu Aug  2 12:02:05 2018

@author: V0010894
"""
#import time
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import boto3
#import feather
#import asyncio

#from get_maxtime_config_asyncio_polling import (
from get_maxtime_config_asyncio import (
        get_veh_det_plans, 
        get_ped_det_plans, 
        get_unit_info,
        get_rms_mainline_parameters,
        get_rms_demand_detectors,
        get_rms_passage_detectors,
        get_rms_queue_detectors
)
from get_maxtime_config_reduce import (
        reduce_det_plans, 
        reduce_ped_plans, 
        reduce_unit_info, 
        reduce_version_info,
        reduce_rms_detectors
)
from get_atspm_detectors import get_atspm_detectors

from get_included_detectors import get_included_detectors

s3 = boto3.client('s3')

# get Good/Authoritative Detector Config (det_config) and write to feather
def get_det_config(ad, mvdp, date_string):
    # -- ---------------------------------
    # -- Included detectors --
    #  SignalID/Detector pairs with vol > 0 over the past year
    #  over a sample of dates between 8am-9am
    
    incld = get_included_detectors().assign(index = 1) #!!!!!
    incld.reset_index().to_feather('included_detectors.feather') #!!!!!
    #incld = pd.read_feather('included_detectors.feather').set_index(['SignalID','Detector']) #!!!!!
    
    # -- -------------------------------------------------- --
    # -- ATSPM Detector Config (from reduce function above) --
  
    adc = ad
    
    try:
        adc = adc[adc.SignalID != 'null']
    except:
        pass
    adc = adc[~adc.Detector.isna()]
    adc = adc[~adc.CallPhase.isna()]
    
    adc.SignalID = adc.SignalID.astype('int')
    adc.Detector = adc.Detector.astype('int')
    adc.CallPhase = adc.CallPhase.astype('int')
    
    adc = adc.set_index(['SignalID','Detector'])
    
    # Exclude those not in "included detectors"
    #adc = adc.join(incld)
    #adc = adc[~adc['Unnamed: 0'].isna()].drop(columns=['Unnamed: 0'])
    
    # -- --------------------------------------------------- --
    # -- Maxtime Detector Plans (from reduce function above) --
    
    mdp = mvdp[['SignalID','Detector','CallPhase','IP']]
    
    mdp.SignalID = mdp.SignalID.astype('int')
    mdp.Detector = mdp.Detector.astype('int')
    mdp.CallPhase = mdp.CallPhase.astype('int')
    
    mdp = mdp.set_index(['SignalID','Detector'])
    
    # Exclude those not in "included detectors"
    #mdp = mdp.join(incld).rename(columns={'index': 'in_cel'})
    mdp = pd.merge(mdp, incld, how='outer', left_index=True, right_index=True)\
            .rename(columns={'index': 'in_cel'})
    mdp = mdp[~mdp['in_cel'].isna()]
    
    # -- --------------------------------------------------------- --
    # -- Combine ATSPM Detector Config and MaxTime Detector Config --

    det_config = pd.merge(adc, mdp, how='outer', 
                          left_index=True, right_index=True, 
                          suffixes=('_atspm', '_maxtime')).sort_index()
    
    det_config.TimeFromStopBar = det_config.TimeFromStopBar.fillna(0).round(1)
    det_config['CallPhase'] = np.where(det_config.CallPhase_maxtime.isna(),
                                       det_config.CallPhase_atspm,
                                       det_config.CallPhase_maxtime).astype('int')
    det_config.LaneNumber = det_config.LaneNumber.astype('double')
    
    #det_config = det_config[((~det_config.CallPhase_maxtime.isna()) & (~det_config.in_cel.isna()))]
    det_config = det_config[~(det_config.CallPhase_atspm.isna() & det_config.CallPhase_maxtime.isna())]
    det_config = det_config[~det_config.in_cel.isna()]

    if 'CountPriority' in det_config.columns:
        det_config.CountPriority = det_config.CountPriority.fillna(max(det_config.CountPriority) + 1)
    
    det_config = det_config.reset_index()
    
    return det_config


#upload to s3, and fill in gaps in past days
def upload_to_s3(filename, key_base, date_):
    date_string = date_.strftime('%Y-%m-%d')
    key = key_base.format(date_string)
    while True:
        print(key)
        s3.upload_file(Filename=filename, 
                   Bucket='gdot-devices', 
                   Key=key)
        date_ = date_ - timedelta(days=1)
        date_string = date_.strftime('%Y-%m-%d')
        key = key_base.format(date_string)
        if 'Contents' in s3.list_objects(Bucket='gdot-devices', Prefix=key):
            break
    os.remove(filename)


if __name__=='__main__':
    
    
    date_ = datetime.today()
    date_string = date_.strftime('%Y-%m-%d')
    
    print('Date:', date_string)
    # ---------------------------------------------------------
    
    
    print('{}: Getting ped detector plans'.format(
            datetime.now().strftime('%x %X')))
    get_ped_det_plans()
    print('{}: Reducing ped detector plans'.format(
            datetime.now().strftime('%x %X')))
    mpdp = reduce_ped_plans()
    mpdp_csv_filename = 'MaxTime_Ped_Plans_{}.csv'.format(
            date_string)
    mpdp.to_csv(mpdp_csv_filename)
    
    #upload to s3
    print('{} Uploading ped detector plans to S3...'.format(
            datetime.now().strftime('%x %X')))
    upload_to_s3(
            mpdp_csv_filename, 
            'maxtime_ped_plans/date={}/MaxTime_Ped_Plans.csv', 
            date_)
    # ---------------------------------------------------------

    
    print('{} Getting Ramp Meter Data...'.format(
            datetime.now().strftime('%x %X')))
    get_rms_mainline_parameters()
    get_rms_demand_detectors()
    get_rms_passage_detectors()
    get_rms_queue_detectors()
    
    rms = reduce_rms_detectors()
    rms_csv_filename = 'RampMeter_Detector_Plans_{}.csv'.format(
            date_string)
    rms.to_csv(rms_csv_filename)
    upload_to_s3(
            rms_csv_filename,
            'maxtime_rampmeter_detectors/date={}/MaxTime_RampMeter_Detectors.csv', 
            date_)
    # ---------------------------------------------------------


    print('{} Getting vehicle detector plans...'.format(
            datetime.now().strftime('%x %X')))
    get_veh_det_plans()  # this takes a long time
    print('{} Reducing vehicle detector plans...'.format(
            datetime.now().strftime('%x %X')))
    mvdp = reduce_det_plans()  # this takes a long time (~28 minutes)
    mvdp_csv_filename = 'MaxTime_Det_Plans_{}.csv'.format(
            date_string)
    mvdp.to_csv(mvdp_csv_filename)
    
    print('{} Uploading MaxTime Detector Config to S3...'.format(
            datetime.now().strftime('%x %X')))
    upload_to_s3(
            mvdp_csv_filename, 
            'maxtime_det_plans/date={}/MaxTime_Det_Plans.csv', 
            date_)
    #mvdp = pd.read_csv('s3://gdot-devices/maxtime_det_plans/date={}/MaxTime_Det_Plans.csv'.format(date_string)).drop(columns=['Unnamed: 0'])
    # ---------------------------------------------------------

    
    print('{} Getting ATSPM Detector Config...'.format(
            datetime.now().strftime('%x %X')))
    ad = get_atspm_detectors(date = date_)
    ad_csv_filename = 'ATSPM_Det_Config_{}.csv'.format(
            date_string)
    ad.to_csv(ad_csv_filename)
    
    # upload to s3
    print('{} Uploading ATSPM Detector Config to S3...'.format(
            datetime.now().strftime('%x %X')))
    upload_to_s3(
            ad_csv_filename, 
            'atspm_det_config/date={}/ATSPM_Det_Config.csv', 
            date_)
    #ad = pd.read_csv('s3://gdot-devices/atspm_det_config/date={}/ATSPM_Det_Config.csv'.format(date_string)).drop(columns=['Unnamed: 0'])
    # ---------------------------------------------------------

    
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
    #det_config.to_feather(dc_filename)
    det_config.to_feather(dc_filename)
    
    # upload to s3
    upload_to_s3(
            dc_filename, 
            'atspm_det_config_good/date={}/ATSPM_Det_Config_Good.feather', 
            date_)
    # ---------------------------------------------------------

    
    print('{} Getting MaxTime Unit Info...'.format(
            datetime.now().strftime('%x %X')))
    get_unit_info()
    ui = reduce_unit_info()
    ui_csv_filename = 'MaxTime_Unit_Info_{}.csv'.format(
            date_string)
    ui.to_csv(ui_csv_filename)
    
    # upload to s3
    print('{} Uploading MaxTime Unit Info to S3...'.format(
            datetime.now().strftime('%x %X')))
    upload_to_s3(
            ui_csv_filename, 
            'maxtime_unit_info/date={}/MaxTime_Unit_Info.csv', 
            date_)
    # ---------------------------------------------------------

    
    print('{} Getting MaxTime Version Info...'.format(
            datetime.now().strftime('%x %X')))
    vi = reduce_version_info()
    vi_csv_filename = 'MaxTime_Version_Info_{}.csv'.format(
            date_string)
    vi.to_csv(vi_csv_filename)
    
    # upload to s3
    print('{} Uploading MaxTime Version Info to S3...'.format(
            datetime.now().strftime('%x %X')))
    upload_to_s3(
            vi_csv_filename, 
            'maxtime_version_info/date={}/MaxTime_Version_Info.csv', 
            date_)
    
    
    