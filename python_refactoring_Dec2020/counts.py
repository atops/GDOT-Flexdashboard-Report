# -*- coding: utf-8 -*-
"""
Created on Thu Dec 10 10:18:54 2020

@author: Alan.Toppen
"""

import pandas as pd
import itertools
import dask
from dask.diagnostics import ProgressBar
from dask.distributed import Client, progress

from atspm_data import (
    get_atspm_s3filenames, 
    get_atspm_data, 
    get_raw_veh_counts, 
    get_raw_ped_counts
)
from detector_configs import (
    get_detector_config_s3filenames, 
    get_detector_config, 
    get_detector_config_vol,
    get_detector_config_sf
)

conf = {'bucket': 'gdot-spm',
        'config_bucket': 'gdot-devices',
        'detector_config_prefix': 'atspm_det_config_good',
        'ped_config_prefix': 'maxtime_ped_plans'}


def get_counts_1hr(counts, config):
    signalids = list(set(counts.SignalID.values))
    config = config[config.SignalID.isin(signalids)]
    
    all_hours = pd.date_range(
        start=pd.Timestamp(f'{date} 00:00:00'), 
        end=pd.Timestamp(f'{date} 23:00:00'), 
        freq='H')
    all_detector_hour_combinations = pd.DataFrame(
        itertools.product(signalids, all_hours, config.Detector), 
        columns=['SignalID', 'Timestamp', 'Detector'])
    
    counts_only_count_detectors = counts[counts.Detector.isin(list(set(config.Detector)))]
    
    counts_1hr = pd.merge(
        all_detector_hour_combinations , 
        counts_only_count_detectors, 
        how='left', on=['SignalID', 'Timestamp', 'Detector']).fillna(0)
    
    counts_1hr.loc[:, 'Volume'] = counts_1hr.loc[:, 'Volume'].astype('int')
    
    return counts_1hr


if __name__=='__main__':

    # client = Client(threads_per_worker=4, n_workers=1)
    client = Client()
    
    date = '2020-12-08'
    
    detector_config_s3filenames = get_detector_config_s3filenames(
        date, conf['config_bucket'], prefix = conf['detector_config_prefix'])
    detector_configs = get_detector_config(
        detector_config_s3filenames)
    
    detector_config_vol = get_detector_config_vol(
        detector_config_s3filenames)
    
    s3filenames = get_atspm_s3filenames(date, conf['bucket'])
    
    # with Pool(4) as pool:
    #     result = pool.map_async(get_atspm_data, s3filenames[1000:1010])
    #     pool.close()
    #     pool.join()
    # atspms = result.get()
    # count_dfs = [get_raw_veh_counts(data, 'h') for data in atspms]
    # count_dfs = [get_raw_ped_counts(data, 'h') for data in atspms]
    
    atspms = [dask.delayed(get_atspm_data)(f) for f in s3filenames]
    veh_count_dfs = [dask.delayed(get_raw_veh_counts)(data, 'h') for data in atspms]
    ped_count_dfs = [dask.delayed(get_raw_ped_counts)(data, 'h') for data in atspms]
    
    v = dask.delayed(pd.concat)(veh_count_dfs)
    p = dask.delayed(pd.concat)(ped_count_dfs)
    
    with ProgressBar():
        v.compute()
    with ProgressBar():
        p.compute()
