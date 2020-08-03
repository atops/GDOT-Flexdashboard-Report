# -*- coding: utf-8 -*-
"""
Created on Tue Apr 14 14:52:13 2020

@author: V0010894
"""

import pandas as pd
import sqlalchemy as sq
from datetime import datetime, timedelta
#from multiprocessing.dummy import Pool
from multiprocessing import Pool
import itertools


muid = os.environ['MAXV_USERNAME']
mpwd = os.environ['MAXV_PASSWORD']

mv_el_engine = sq.create_engine(
        'mssql+pyodbc://{}:{}@MaxView_EventLog'.format(muid, mpwd), 
        pool_size=50)


def get_signals(date_):
    
    print(f'{date_.strftime("%F")}: Getting signals..')
    
    start_date = date_.strftime('%F')
    end_date = (date_+ timedelta(days=1)).strftime('%F')
    
    signals_query = f"""
            select distinct SignalID
            from Controller_Event_Log
            where TimeStamp BETWEEN '{start_date}' AND '{end_date}'
            """
    with mv_el_engine.connect() as conn:
        signals = pd.read_sql(sql=signals_query, con=conn)
    
    return signals


def get_gaps_signal(signal, date_, gapsize_seconds):

    print(signal, end='|')

    start_date = date_.strftime('%F')
    end_date = (date_+ timedelta(days=1)).strftime('%F')

    timestamps_query = f"""
        select distinct TimeStamp
        from Controller_Event_Log
        where TimeStamp BETWEEN '{start_date}' AND '{end_date}'
        and SignalID = '{signal}'
        """

    with mv_el_engine.connect() as conn:
        df = pd.read_sql(sql=timestamps_query, con=conn)

    df = (pd.concat([df,
                     pd.DataFrame({'TimeStamp': [date_]}),
                     pd.DataFrame({'TimeStamp': [date_ + timedelta(days=1)]})])
          .sort_values(['TimeStamp'])
          .assign(SignalID = signal,
                  gap = lambda x: (x.TimeStamp - x.TimeStamp.shift(1)).dt.total_seconds()))
    df['comm_gap'] = df['gap']
    df.loc[df.gap < gapsize_seconds, "comm_gap"] = 0
    df['is_gap'] = 0
    df.loc[df.comm_gap > 0, 'is_gap'] = 1

    return (df.groupby(['SignalID'])
            .agg({'comm_gap': ['sum'], 
                  'is_gap': ['sum']})
            .reset_index())

    
def get_gaps(date_, gapsize_seconds=900):

    date_ = datetime(date_.year, date_.month, date_.day)
    
    signals = get_signals(date_).SignalID.values
    print(f'{len(signals)} signals')
    
    with Pool(processes=50) as pool:
        results = pool.starmap_async(get_gaps_signal, list(itertools.product(signals, [date_], [gapsize_seconds])))
        pool.close()
        pool.join()
    dfs = results.get()
    
    df = pd.concat(dfs).sort_values(['SignalID'])
    
    df = pd.DataFrame(
                {'Date': date_,
                 'SignalID': df.SignalID, 
                 'gap_time': df.comm_gap['sum'], 
                 'gap_count': df.is_gap['sum']})
    
    return df

if __name__=='__main__':

    yesterday = datetime.today() - timedelta(days=1)
    yest_str = yesterday.strftime('%F')
    
    df = get_gaps(yesterday)
    df.to_parquet(
        f's3://gdot-spm/mark/comm_uptime_new/date={yest_str}/gaps_{yest_str}.parquet')
    
    """    
    for d in pd.date_range('2020-05-11', '2020-05-26'):
        df = get_gaps(d)
        df.to_parquet(
            f's3://gdot-spm/mark/comm_uptime_new/date={d.strftime("%F")}/gaps_{d.strftime("%F")}.parquet')
    """