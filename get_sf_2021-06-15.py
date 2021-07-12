# -*- coding: utf-8 -*-
"""
Created on Tue Jun 15 17:18:59 2021

@author: Alan.Toppen
"""

import pandas as pd
import boto3
import io
import os

# TODO: Adapt this for sf
def get_det_config(date_):
    '''
    date_ [Timestamp]
    '''
    date_str = date_.strftime('%Y-%m-%d')
    
    bd_key = f's3://gdot-spm/mark/bad_detectors/date={date_str}/bad_detectors_{date_str}.parquet'
    bd = pd.read_parquet(bd_key)
    bd['SignalID'] = bd.SignalID.astype('int64')
    bd['Detector'] = bd.Detector.astype('int64')
    bd['date'] = np.asarray(bd.date)

    dc_key = 'atspm_det_config_good/date={d}/ATSPM_Det_Config_Good.feather'
    with io.BytesIO() as data:
        boto3.resource('s3').Bucket('gdot-devices').download_fileobj(
                Key=dc_key.format(d=date_str), Fileobj=data)
        dc = pd.read_feather(data)
    dc = dc[['SignalID', 'Detector', 'DetectionTypeDesc', 'TimeFromStopBar']]
    dc.loc[dc.DetectionTypeDesc.isna(), 'DetectionTypeDesc'] = '[]'

    df = (pd.merge(dc, bd, how='outer', on=['SignalID','Detector'])
          .fillna(value={'Good_Day': 1, 'date': date_str}))
    df = df.loc[df.Good_Day==1].drop(columns=['Good_Day'])

    return df


def main(start_date, end_date, interval='1H'):

    dates = pd.date_range(start_date, end_date, freq='1D')
    #dates = pd.date_range('2020-02-01', '2020-02-16')

    for date_ in dates:
      try:
        t0 = time.time()
        date_str = date_.strftime('%Y-%m-%d')
        print(date_str)
    
        print('Getting detector configuration...', end='')
        det_config = get_det_config(date_)
        print('done.')
        print('Getting signal...', end='')
        signalids = get_signalids(date_)
        print('done.')


# SPM Arrivals on Green using Utah method -- modified for use with dbplyr on AWS Athena
def get_sf_utah(signalid, date_, det_config, first_seconds_of_red=5, interval='1H'):

    def get_occupancy(de, df_interval):
        # Green Interval (repeat for Start-of-Red)            
        x = pd.merge(de, df_interval, left_index=True, right_index=True)
        a = x[['DetOn','IntervalStart']].max(axis=1)
        b = x[['DetOff','IntervalEnd']].min(axis=1)
        
        x['overlap'] = (b-a).dt.total_seconds()
        x.loc[x.overlap<0, 'overlap'] = 0
        
        x = x.reset_index(level='CycleStart').set_index('Detector', append=True)
        
        
        x['interval'] = (x.IntervalEnd - x.IntervalStart).dt.total_seconds()
        x = x.set_index(['CycleStart'], append=True)
        
        y = x.groupby(level=['SignalID','Phase','Detector','CycleStart']).agg({'overlap': sum, 'interval': max})
        y['occ'] = y['overlap']/y['interval']
        z = y.groupby(level=['SignalID','Phase','CycleStart']).max()
        
        return z
    
    print("Pulling data...")

    try:
        date_str = date_.strftime('%Y-%m-%d')
        
        all_hours = pd.date_range(date_, date_ + pd.Timedelta(1, unit='days'), freq=interval)


        de_fn = f'../detections/date={date_str}/signal={signalid}/de_{signalid}_{date_str}.parquet'
        if os.path.exists(de_fn):
            detection_events = pd.read_parquet(de_fn).drop_duplicates()
        else:
            de_fn = f's3://gdot-spm/detections/date={date_str}/de_{signalid}_{date_str}.parquet'
            detection_events = pd.read_parquet(de_fn).drop_duplicates()

        cd_fn = f'../cycles/date={date_str}/signal={signalid}/cd_{signalid}_{date_str}.parquet'
        if os.path.exists(cd_fn):
            detection_events = pd.read_parquet(cd_fn).drop_duplicates()
        else:
            cd_fn = f's3://gdot-spm/cycles/date={date_str}/cd_{signalid}_{date_str}.parquet'
            cd = pd.read_parquet(cd_fn).drop_duplicates()
            
        
        
        de = (pd.merge(
                detection_events,
                det_config[det_config.DetectionTypeDesc.str.contains('Stop Bar Presence')],
                on=['SignalID', 'Detector', 'date'],
                how='left'))
        de = de[~de.DetectionTypeDesc.isna()]
        de = de[~de.TimeFromStopBar.isna()]
        de['DetOn'] = de['DetTimeStamp']
        de['DetOff'] = de['DetTimeStamp'] + pd.to_timedelta(de['DetDuration'], unit='s')
        de = de[['SignalID','Phase','Detector','CycleStart','PhaseStart','EventCode','DetOn','DetOff']]
        de = de.set_index(['SignalID','Phase','CycleStart'])

        if de.empty:
            print('#', end='')
            return pd.DataFrame()
        else:
            cd = cd[cd.EventCode.isin([1,9])]
            cd = cd.sort_values(['SignalID', 'Phase', 'CycleStart', 'PhaseStart'])
            
            
            grn_interval = (cd[cd.EventCode==1]
                            .rename(columns={
                                'PhaseStart': 'IntervalStart', 
                                'PhaseEnd': 'IntervalEnd'})
                            .set_index(['SignalID','Phase','CycleStart']))
            grn_interval = grn_interval[['IntervalStart', 'IntervalEnd']]
            
            
            sor_interval = (cd[cd.EventCode==9]
                            .rename(columns={
                                'PhaseStart': 'IntervalStart'})
                            .drop(columns=['EventCode'])
                            .set_index(['SignalID','Phase','CycleStart']))
            sor_interval['IntervalEnd'] = sor_interval['IntervalStart'] + pd.Timedelta(first_seconds_of_red, unit='s')
            sor_interval = sor_interval[['IntervalStart', 'IntervalEnd']]

        
            w = pd.merge(
                left=get_occupancy(de, grn_interval), 
                right=get_occupancy(de, sor_interval), 
                left_index=True, 
                right_index=True, 
                how='outer', 
                suffixes=['_grn', '_sor'])
            w = w.fillna(0)
            w = w.groupby(level=['SignalID','CycleStart']).max()

            w['sf'] = 0
            w.loc[(w.occ_grn > 0.8) & (w.occ_sor > 0.8), 'sf'] = 1

            
    except Exception as e:
        print(e)
    
    
    ## ---
    
    get_occupancy <- function(de_dt, int_dt, interval_) {
        occdf <- foverlaps(de_dt, int_dt, type = "any") %>% 
            filter(!is.na(IntervalStart)) %>% 
            
            transmute(
                SignalID = factor(SignalID),
                Phase,
                Detector = as.integer(as.character(Detector)),
                CycleStart,
                IntervalStart,
                IntervalEnd,
                int_int = lubridate::interval(IntervalStart, IntervalEnd), 
                occ_int = lubridate::interval(DetOn, DetOff), 
                occ_duration = as.duration(intersect(occ_int, int_int)),
                int_duration = as.duration(int_int))
        
        occdf <- full_join(interval_, 
                           occdf, 
                           by = c("SignalID", "Phase", 
                                  "CycleStart", "IntervalStart", "IntervalEnd")) %>% 
            tidyr::replace_na(
                list(Detector = 0, occ_duration = 0, int_duration = 1)) %>%
            mutate(SignalID = factor(SignalID),
                   Detector = factor(Detector),
                   occ_duration = as.numeric(occ_duration),
                   int_duration = as.numeric(int_duration)) %>%
            
            group_by(SignalID, Phase, CycleStart, Detector) %>%
            summarize(occ = sum(occ_duration)/max(int_duration),
                      .groups = "drop_last") %>%
            
            summarize(occ = max(occ),
                      .groups = "drop") %>%
            
            mutate(SignalID = factor(SignalID),
                   Phase = factor(Phase))
        
        occdf
    }
    
    grn_occ <- get_occupancy(de_dt, gr_dt, grn_interval) %>% 
        rename(gr_occ = occ)
    cat('.')
    sor_occ <- get_occupancy(de_dt, sr_dt, sor_interval) %>% 
        rename(sr_occ = occ)
    cat('.\n')
    
    
    
    df <- full_join(grn_occ, sor_occ, by = c("SignalID", "Phase", "CycleStart")) %>%
        mutate(sf = if_else((gr_occ > 0.80) & (sr_occ > 0.80), 1, 0))
    
    # if a split failure on any phase
    df0 <- df %>% group_by(SignalID, Phase = factor(0), CycleStart) %>% 
        summarize(sf = max(sf), .groups = "drop")
    
    sf <- bind_rows(df, df0) %>% 
        mutate(Phase = factor(Phase)) %>%
        
        group_by(SignalID, Phase, hour = floor_date(CycleStart, unit = interval)) %>% 
        summarize(cycles = n(),
                  sf_freq = sum(sf, na.rm = TRUE)/cycles, 
                  sf = sum(sf, na.rm = TRUE),
                  .groups = "drop") %>%
        
        transmute(SignalID, 
                  CallPhase = Phase, 
                  Date_Hour = ymd_hms(hour),
                  Date = date(Date_Hour),
                  DOW = wday(Date),
                  Week = week(Date),
                  sf = as.integer(sf),
                  cycles = cycles,
                  sf_freq = sf_freq)
    
    sf
}