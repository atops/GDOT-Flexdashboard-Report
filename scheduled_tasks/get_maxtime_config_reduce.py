# -*- coding: utf-8 -*-
"""
Python 3.6.0 |Anaconda custom (64-bit)| (default, Dec 23 2016, 11:57:41) 
[MSC v.1900 64 bit (AMD64)] on win32

Created on Wed May 31 16:21:57 2017

@author: Alan.Toppen
"""

from bs4 import BeautifulSoup
import pandas as pd
import re
import glob
import os
from datetime import datetime
import sqlalchemy as sq
from multiprocessing.dummy import Pool
#from multiprocessing import Pool
#from dask.distributed import Client

from get_intersections import get_maxview_intersections

from dask import delayed, compute


pd.options.display.width = 120


def read_atspm_table(table_name):
    engine = sq.create_engine('mssql+pyodbc://{}:{}@atspm'.format(
                    os.environ['ATSPM_USERNAME'], 
                    os.environ['ATSPM_PASSWORD']), 
                pool_size=20)
    
    with engine.connect() as con:
        df = pd.read_sql_table(table_name, con=con)
    return df

atspm_signals = (read_atspm_table('Signals')
                    .query("SignalID != 'null'")
                    .assign(SignalID = lambda x: x.SignalID.astype('int64')))

def is_header_dummy(ths):
    return True
def is_row_dummy(ths, tds):
    return True

def is_intersection_header(ths):
    
    if len(ths)==5:
        return True
    else:
        return False

def is_intersection_row(ths, tds):
    
    if len(tds)==5:
        return True
    else:
        return False

def is_zone_header(ths):
    '''
    For Detector Zones, this pattern identifies a header in the html
    of the table we want to grab.
    '''
    if len(ths)>0 and ths[0].text == 'Zone':
        return True
    else:
        return False

    
def is_zone_row(ths, tds):
    '''
    For Detector Zones, this pattern identifies a row in the html
    of the table we want to grab.
    '''
    if len(ths)==1 and len(tds)>=5 and len(tds)<10: 
        return True
    else:
        return False


def is_plan_header(ths):
    '''
    For Detector Plans, this pattern identifies a header in the html
    of the table we want to grab.
    '''
    if len(ths)>0 and ths[0].text == 'Detector':
        return True
    else:
        return False


def is_plan_row(ths, tds):
    '''
    For Detector Plans, this pattern identifies a row in the html
    of the table we want to grab.
    '''
    if len(ths)==1 and len(tds)>10 and len(tds)<30:
        return True
    else:
        return False


def is_ped_header(ths):
    '''
    For Detector Plans, this pattern identifies a header in the html
    of the table we want to grab.
    '''
    if len(ths)>0 and ths[0].text == 'Detector':
        return True
    else:
        return False


def is_ped_row(ths, tds):
    '''
    For Detector Plans, this pattern identifies a row in the html
    of the table we want to grab.
    '''
    if len(ths)==1 and len(tds)>6 and len(tds)<15:
        return True
    else:
        return False


def is_time_header(ths):
    '''
    For Detector Plans, this pattern identifies a header in the html
    of the table we want to grab.
    '''
    if len(ths)>0 and re.search('Current', ths[0].text):
        return True
    else:
        return False


def is_time_row(ths, tds):
    '''
    For Detector Plans, this pattern identifies a row in the html
    of the table we want to grab.
    '''
    if len(tds)==1 and re.match('\S+ \S+ .*', tds[0].text):
        return True
    else:
        return False


def is_tz_header(ths):
    '''
    For Detector Plans, this pattern identifies a header in the html
    of the table we want to grab.
    '''
    if len(ths)>0 and ths[0].text=='Time Zone':
        return True
    else:
        return False


def is_tz_row(ths, tds):
    '''
    For Detector Plans, this pattern identifies a row in the html
    of the table we want to grab.
    '''
    if len(tds)==2 and re.match('GMT', tds.option[0].text):
        return True
    else:
        return False


def is_param_header(ths):
    '''
    For Detector Plans, this pattern identifies a header in the html
    of the table we want to grab.
    '''
    if len(ths)==1:
        return True
    else:
        return False

def is_param_row(ths, tds):
    '''
    For Detector Plans, this pattern identifies a row in the html
    of the table we want to grab.
    '''
    if len(tds)==1:
        return True
    else:
        return False

def is_version_header(ths):
    '''
    For Version Information, this pattern identifies a header in the html
    of the table we want to grab.
    '''
    if len(ths)>0 and ths[0].text=='Module No.':
        return True
    else:
        return False


def is_version_row(ths, tds):
    '''
    For Version Information, this pattern identifies a row in the html
    of the table we want to grab.
    '''
    if len(tds)==3:
        return True
    else:
        return False

def is_unit_header(ths):
    '''
    For Unit Information, this pattern identifies a header in the html
    of the table we want to grab.
    '''
    if len(ths)>0 and ths[0].text=='Controller ID':
        return True
    else:
        return False
    
    
def is_unit_row(ths, tds):
    '''
    For Unit Information, this pattern identifies a row in the html
    of the table we want to grab.
    '''
    if len(tds)==5:
        return True
    else:
        return False
    
        
def is_point_info_header(ths):
    '''
    For Detector Plans, this pattern identifies a header in the html
    of the table we want to grab.
    '''
    if len(ths)>0 and ths[0].text=='Input Point':
        return True
    else:
        return False


def is_point_info_row(ths, tds):
    '''
    For Detector Plans, this pattern identifies a row in the html
    of the table we want to grab.
    '''
    if len(ths)==1 and len(tds) > 0:
        return True
    else:
        return False


def is_preempt_header(ths):
    '''
    '''
    if len(ths) == 5 and ths[0].text == 'Preempt':
        return True
    else:
        return False


def is_preempt_row(ths, tds):
    '''
    '''
    if len(ths)==1 and len(tds) == 4:
        return True
    else:
        return False


def is_rms_ml_header(ths):
    '''
    For Detector Zones, this pattern identifies a header in the html
    of the table we want to grab.
    '''
    if len(ths)==5:
        return True
    else:
        return False
    
    
def is_rms_ml_row(ths, tds):
    '''
    For Detector Zones, this pattern identifies a row in the html
    of the table we want to grab.
    '''
    if len(ths)==1 and len(tds)==4: 
        return True
    else:
        return False


def is_rms_demand_header(ths):
    '''
    For Detector Zones, this pattern identifies a header in the html
    of the table we want to grab.
    '''
    if len(ths)==8:
        return True
    else:
        return False
    
    
def is_rms_demand_row(ths, tds):
    '''
    For Detector Zones, this pattern identifies a row in the html
    of the table we want to grab.
    '''
    if len(ths)==1 and len(tds)==7: 
        return True
    else:
        return False


def is_rms_queue_header(ths):
    '''
    For Detector Zones, this pattern identifies a header in the html
    of the table we want to grab.
    '''
    if len(ths)==5:
        return True
    else:
        return False
    
    
def is_rms_queue_row(ths, tds):
    '''
    For Detector Zones, this pattern identifies a row in the html
    of the table we want to grab.
    '''
    if len(ths)==1 and len(tds)==4: 
        return True
    else:
        return False    


def get_maxtime_table_df(html_fn, is_header, is_row):
    '''
    Take a file with raw html and functions used to identify
    the header of the table we want, and the rows of the table
    we want, and return a Pandas dataframe of that html table.
    '''
    # get IP address using regular expression on file name
    ip = re.search('\d+_\d+_\d+_\d+', html_fn).group().replace('_','.')

    with open(html_fn) as f:
        html = f.read()

    return maxtime_table_html(ip, html, is_header, is_row)


def maxtime_table_html(ip, html, is_header, is_row):

    soup = BeautifulSoup(html, 'html.parser')

    dicts = []
    header = []
    
    # Loop through all rows denoted by <tr>...</tr>
    for tr in soup.find_all('tr'):
    
        # Get header
        ths = tr.find_all('th')
        # Get the right header
        if is_header(ths):
            header = ['IP'] + [th.text for th in ths]
    
        # Get column data from the tables rows
        tds = tr.find_all('td')
        # From the right table rows
        if is_row(ths, tds):
            try:
                row = [str(ip), ths[0].text] # IP, First column entry in table
            except:
                row = [str(ip)]
            for td in tds:
                if td.input: 
                    row.append(td.input['value'])
                elif td.option: #td.value ### changed 3/9/18
                    row = row + [opt.text for opt in td.find_all('option') if opt.get('selected')=='selected']
                
                else: # td.div
                    row.append(td.text.strip())

            dicts.append(dict(zip(header, row)))

    # if there is a header and no data, dicts will be empty
    if dicts:
        df = pd.DataFrame(dicts)[header]
    else:
        df = pd.DataFrame()

    return df

"""
def get_maxtime_table_df(html_fn, is_header, is_row):
    '''
    Take a file with raw html and functions used to identify
    the header of the table we want, and the rows of the table
    we want, and return a Pandas dataframe of that html table.
    '''    
    with open(html_fn) as f:
        html = f.read()
    soup = BeautifulSoup(html, 'html.parser')
    
    # get IP address using regular expression on file name
    ip = re.search('\d+_\d+_\d+_\d+', html_fn).group().replace('_','.')
    
    dicts = []
    header = []
    
    # Loop through all rows denoted by <tr>...</tr>
    for tr in soup.find_all('tr'):
    
        # Get header
        ths = tr.find_all('th')
        # Get the right header
        if is_header(ths):
            header = ['IP'] + [th.text for th in ths]
    
        # Get column data from the tables rows
        tds = tr.find_all('td')
        # From the right table rows
        if is_row(ths, tds):
            try:
                row = [str(ip), ths[0].text] # IP, First column entry in table
            except:
                row = [str(ip)]
            for td in tds:
                if td.input: 
                    row.append(td.input['value'])
                elif td.option: #td.value ### changed 3/9/18
                    row = row + [opt.text for opt in td.find_all('option') if opt.get('selected')=='selected']
                
                else: # td.div
                    row.append(td.text.strip())

            dicts.append(dict(zip(header, row)))

    # if there is a header and no data, dicts will be empty
    if dicts:
        df = pd.DataFrame(dicts)[header]
    else:
        df = pd.DataFrame()

    return df
"""

def get_detector_plans_df(html_fn):
    df_plans = get_maxtime_table_df(html_fn, is_plan_header, is_plan_row)
    if 'Call Phase' in df_plans.columns:
        return df_plans[df_plans['Call Phase'] != '0']\
                    .filter(items=['IP',
                                   'Detector',
                                   'Call Phase',
                                   'Call Overlap',
                                   'Additional Call Phases',
                                   'Switch Phase',
                                   'Delay',
                                   'Extend',
                                   'Queue Limit',
                                   'Extension Hold',
                                   'No Activity',
                                   'Max Presence',
                                   'Erratic Count',
                                   'Erratic Counts',
                                   'Fail Time',
                                   'Failed Recall',
                                   'Failed Link',
                                   'Description'])

def get_detector_zones_df(html_fn):
    return get_maxtime_table_df(html_fn, is_zone_header, is_zone_row)


def get_intersections_df(html_fn):
    return get_maxtime_table_df(html_fn, is_intersection_header, is_intersection_row)


def get_firmware_df(html_fn):
    
    # get IP address using regular expression on file name
    ip = re.search('\d+_\d+_\d+_\d+', html_fn).group().replace('_','.')

    try:
        with open(html_fn) as f:
            html = f.read()
            soup = BeautifulSoup(html, 'html.parser')
        
            firmware = soup.find('h1').text
    
        return pd.DataFrame([{'IP': ip, 'Firmware': firmware}])
    except:
        return pd.DataFrame([{'IP': ip, 'Firmware': ''}])


def get_ped_plans_df(html_fn):
    df_peds = get_maxtime_table_df(html_fn, is_ped_header, is_ped_row)
    if 'Call Phase' in df_peds.columns:
        return df_peds[df_peds['Call Phase'] != '0']


def get_time_df(html_fn):
    return get_maxtime_table_df(html_fn, is_time_header, is_time_row)


def get_tz_df(html_fn):
    return get_maxtime_table_df(html_fn, is_tz_header, is_tz_row)


def get_unit_df(html_fn):
    return get_maxtime_table_df(html_fn, is_unit_header, is_unit_row)


def get_unit_from_html(ip, html):
    return maxtime_table_html(ip, html, is_unit_header, is_unit_row)


def get_id_from_unit_html(ip, html):
    ui = get_unit_from_html(ip, html)
    cid = ui.loc[0, 'Controller ID'] + ' - ' + \
          ui.loc[0, 'Main Street'] + ' & ' + \
          ui.loc[0, 'Side Street']
    return cid
    

def point_info_df(html_fn):
    return get_maxtime_table_df(html_fn, is_point_info_header, is_point_info_row)


def get_preempt_df(html_fn):
    return get_maxtime_table_df(html_fn, is_preempt_header, is_preempt_row)


def get_rms_ml_df(html_fn):
    df = get_maxtime_table_df(html_fn, is_rms_ml_header, is_rms_ml_row)
    df0 = df.drop(columns=['IP','Lane']).transpose()
    df0.columns = df.Lane.values
    df0 = df0.reset_index().rename(columns={'index': 'Lane'})
    df0.insert(1, 'Detector Type', 'Mainline')
    df0.insert(0, 'IP', df.IP.values[0])
    return df0


def get_rms_demand_df(html_fn):
    df = get_maxtime_table_df(html_fn, is_rms_demand_header, is_rms_demand_row)
    df.insert(1, 'Detector Type', 'Demand')
    return df[df['Detector Number']!='0']


def get_rms_passage_df(html_fn):
    df = get_maxtime_table_df(html_fn, is_rms_demand_header, is_rms_demand_row)
    df.insert(1, 'Detector Type', 'Passage')
    return df[df['Detector Number']!='0']


def get_rms_queue_df(html_fn):
    df = get_maxtime_table_df(html_fn, is_rms_queue_header, is_rms_queue_row)
    df0 = df.drop(columns=['IP']).set_index(['Queue']).transpose()
    df0 = df0.reset_index().rename(columns={'index': 'Queue'})
    df0.columns.name = None
    df0.insert(1, 'Detector Type', 'Queue')
    df0.insert(0, 'IP', df.IP.values[0])
    
    # Get Lane from dropdown. Specific to each page, which is a separate file
    with open(html_fn, 'r') as f:
        html = f.read()
    soup = BeautifulSoup(html, 'html.parser')
    options = [div.findAll('option') for div in soup.findAll('div') if div.has_attr('class')][0]
    lane = [opt.text for opt in options if opt.has_attr('selected')][0]
    df0.insert(1, 'Lane', lane)
    
    return df0[(df0['Detector Number']!='0') & (df0['Detect Mode']!='Disabled')]
    
def get_version_df(html_fn):
    
    def func(matchobj):
        return '<td>{}</td>'.format(matchobj.group(1))

    ss = '<td>.*?value="(.*?)"[\s\S]*?</td>'
    ip = re.search('\d+_\d+_\d+_\d+', html_fn).group().replace('_','.')

    try:    
        with open(html_fn, 'r') as f:
            html = f.read()

        html2 = re.sub(ss, func, html)
        dfs = pd.read_html(html2, attrs={'class': 'datasheet'})
        dfs[1].insert(0, 'IP', ip)
        return dfs[1]
    except Exception as e:
        print('{}: {}'.format(os.path.basename(html_fn), e))


def get_params_df(html_fn):
    '''
    Take a file with raw html and functions used to identify
    the header of the table we want, and the rows of the table
    we want, and return a Pandas dataframe of that html table.
    '''    
    with open(html_fn) as f:
        html = f.read()
    soup = BeautifulSoup(html, 'html.parser')
    
    # get IP address using regular expression on file name
    ip = re.search('\d+_\d+_\d+_\d+', html_fn).group().replace('_','.')
    
    header = 'IP'
    row = ip
    dict_ = {header: row}
    
    # Loop through all rows denoted by <tr>...</tr>
    for tr in soup.find_all('tr'):
        
        print(tr) # debug step
    
        # Get header
        ths = tr.find_all('th')
        # Get the right header
        if len(ths)==1:
            header = ths[0].text #.extend([th.text for th in ths])
    
            # Get column data from the tables rows
            tds = tr.find_all('td')
            # From the right table rows
            if len(tds)==1:
                for td in tds:
                    if td.input:
                        row = td.input['value']
                    elif td.select: #else: #except:
                        row = [opt.text.strip() for opt in td.find_all('option') if opt.get('selected')=='selected']
                        row = row[0]
                    else: # td.div
                        row = td.text.strip()
                        print(row)
    
                dict_[header] = row

    # if there is a header and no data, dicts will be empty
    if dict_:
        df = pd.DataFrame([dict_])
    else:
        df = pd.DataFrame()

    return df


def reduce_ped_plans():
    
    path_name = 'ped_plan_html'
    base_path = os.path.expanduser('~')
    base_path = os.path.join(base_path, 'Code', 'GDOT', path_name)
    print(base_path)

    # ------- MaxTime Ped Detector Plan Data -----------
    fns = glob.glob(os.path.join(base_path, '*'))
    
    #    client = Client()
    #    x = client.map(get_ped_plans_df, fns)
    #    df_peds_list = client.gather(x)
    #    df_peds = pd.concat(df_peds_list)
    #    client.close()
    
    print('Maxtime 1 peds')
    with Pool(processes=20) as pool:
        results = pool.map_async(get_ped_plans_df, fns)
        pool.close()
        pool.join()
    df_peds_list = results.get()
    df_peds = pd.concat(df_peds_list, sort=True)
    
    
    
    # Read ped config from Maxtime 2 controllers
    #maxtime2_fns = glob.glob(os.path.join(base_path, '../ped_plan_maxtime2/*.parquet'))
    maxtime2_fns = glob.glob(os.path.join(base_path, '../ped_plan_maxtime2/*.csv'))
    
    print('Maxtime 2 peds')
    with Pool(processes=20) as pool:
        #results = pool.map_async(pd.read_parquet, maxtime2_fns)
        results = pool.map_async(pd.read_csv, maxtime2_fns)
        pool.close()
        pool.join()
    df_plan_list = results.get()
    df_peds_maxtime2 = pd.concat(df_plan_list, sort=True)
    
    #df_plans_maxtime2 = pd.concat([pd.read_parquet(fn) for fn in maxtime2_fns])
    
    # Remove signals from Maxtime 1 controllers config that are now in Maxtime 2
    df_peds_maxtime1 = df_peds[~df_peds.IP.isin(df_peds_maxtime2.IP.values)]
    
    # Concatenate config files together
    df_peds = pd.concat([df_peds_maxtime1, df_peds_maxtime2], sort=True)
    

    ped = (pd.merge(left=df_peds.filter(items=['IP','Detector','Call Phase']), 
                    right=atspm_signals.filter(['SignalID', 'IPAddress', 'PrimaryName','SecondaryName']),
                    left_on=['IP'], 
                    right_on=['IPAddress'])
             .assign(Detector = lambda x: x.Detector.astype('int64'),
                     CallPhase = lambda x: x['Call Phase'].astype('int64'))
             .filter(items=['SignalID','IP', 'PrimaryName','SecondaryName','Detector','CallPhase'])
             .drop_duplicates())
    
    
    
    return ped
    
    
def reduce_det_plans():
    path_name = 'det_plan_html'
    base_path = os.path.expanduser('~')
    base_path = os.path.join(base_path, 'Code', 'GDOT', path_name)
    
    # ------- Detector Plan Data -----------    
    fns = glob.glob(os.path.join(base_path, '*plan*.html'))
    print(len(fns))
    #    client = Client()
    #    x = client.map(get_detector_plans_df, fns)
    #    df_plan_list = client.gather(x)
    #    df_plans = pd.concat(df_plan_list, sort=True)
    #    client.close()
    
    with Pool(processes=50) as pool1:
        results = pool1.map_async(get_detector_plans_df, fns)
        pool1.close()
        pool1.join()
    df_plan_list = results.get()
    df_plans = pd.concat(df_plan_list, sort=True)
    
    # Filter out Call Phase == 0
    df_plans = df_plans.drop_duplicates()
    print(df_plans.head())
    
    # Read det config from Maxtime 2 controllers
    #maxtime2_fns = glob.glob(os.path.join(base_path, '../det_plan_maxtime2/*.parquet'))
    maxtime2_fns = glob.glob(os.path.join(base_path, '../det_plan_maxtime2/*.csv'))
    
    #with Pool(processes=100) as pool2:
    #    results = pool2.map_async(pd.read_parquet, maxtime2_fns)
    #    pool2.close()
    #    pool2.join()
    #df_plan_list = results.get()
    #df_plan_list = [pd.read_parquet(fn) for fn in maxtime2_fns]
    df_plan_list = [pd.read_csv(fn) for fn in maxtime2_fns]
    df_plans_maxtime2 = pd.concat(df_plan_list, sort=True)
    df_plans_maxtime2 = df_plans_maxtime2[~df_plans_maxtime2['Call Phase'].isna()]
    print(df_plans_maxtime2.head())
    
    #df_plans_maxtime2 = pd.concat([pd.read_parquet(fn) for fn in maxtime2_fns])
    
    # Remove signals from Maxtime 1 controllers config that are now in Maxtime 2
    df_plans_maxtime1 = df_plans[~df_plans.IP.isin(df_plans_maxtime2.IP.values)]
    print(df_plans_maxtime1.head())
    
    # Concatenate config files together
    df_plans = pd.concat([df_plans_maxtime1, df_plans_maxtime2], sort=True)
    
    engine_maxv = sq.create_engine('mssql+pyodbc://{}:{}@MaxView'.format(os.environ['MAXV_USERNAME'], os.environ['MAXV_PASSWORD']), pool_size=20)
    maxt = get_maxview_intersections(engine_maxv)
    
    #df = pd.merge(maxt, df_plans, how = 'outer', left_on=['HostAddress'], right_on=['IP'])

    """
    engine = sq.create_engine('mssql+pyodbc://{}:{}@sqlodbc'.format(os.environ['ATSPM_USERNAME'], os.environ['ATSPM_PASSWORD']), pool_size=20)
    
    with engine.connect() as conn:

        atspm = (pd.read_sql_table('Signals', con=conn)
                   .query("SignalID != 'null'")
                   .assign(SignalID = lambda x: x.SignalID.astype('int64')))
    """

    veh = (pd.merge(left=df_plans.filter(items=['IP','Detector','Call Phase']), 
                    #right=atspm.filter(['SignalID', 'IPAddress', 'PrimaryName','SecondaryName']),
                    right=maxt.reset_index().filter(['SignalID','HostAddress','Name']),
                    left_on=['IP'], 
                    #right_on=['IPAddress'])
                    right_on=['HostAddress'])
             .assign(Detector = lambda x: x.Detector.astype('int64'),
                     CallPhase = lambda x: x['Call Phase'].astype('int64'))
             .filter(items=['SignalID','IP', 'Name','Detector','CallPhase'])
             .drop_duplicates()
             .sort_values(['SignalID','Detector']))
             
    """
    df = clean_df_db_dups(veh, 
                          'DetectorConfig', engine, 
                          dup_cols=['SignalID','IP','Detector','CallPhase'], 
                          filter_categorical_col='SignalID')
    print('{} new rows to insert...'.format(len(df)))


    #insert_to_db(df, 'DetectorConfig', engine) # This is slow
    insert_to_db_bcp(df, 'DetectorConfig')
    #veh.to_csv('c:/users/alan.toppen/Code/GDOT/MaxTime Detector Plan Data ATSPM 2017-12-01.csv')
    """
    return veh


def reduce_unit_info():
    path_name = 'unit_info'
    base_path = os.path.expanduser('~')
    base_path = os.path.join(base_path, 'Code', 'GDOT', path_name)

    fns = glob.glob(os.path.join(base_path, '*_unit_info.html'))
    #df_unit = pd.concat([get_unit_df(fn) for fn in fns])
    
    results = [delayed(get_unit_df)(fn) for fn in fns]
    df_unit = pd.concat(compute(*results))
    
    return df_unit


def reduce_version_info():
    
    path_name = 'unit_info'
    base_path = os.path.expanduser('~')
    base_path = os.path.join(base_path, 'Code', 'GDOT', path_name)

    fns = glob.glob(os.path.join(base_path, '*_unit_info.html'))

    df_version_info = pd.concat([get_version_df(fn) for fn in fns])
    df_version_info = df_version_info[df_version_info['Module No.']==1]
    
    return df_version_info


def reduce_rms_detectors():
        basepath = 'c:/Users/V0010894/Code/GDOT/ramp_meters'
        fns = glob.glob(os.path.join(basepath, 'mainline_parameters', '*.html'))
        df_ml = pd.concat([get_rms_ml_df(fn) for fn in fns])
            
        fns = glob.glob(os.path.join(basepath, 'demand_detectors', '*.html'))
        df_dem = pd.concat([get_rms_demand_df(fn) for fn in fns])
        
        fns = glob.glob(os.path.join(basepath, 'passage_detectors', '*.html'))
        df_pass = pd.concat([get_rms_passage_df(fn) for fn in fns])
        
        fns = glob.glob(os.path.join(basepath, 'metered_queues', '*.html'))
        df_qu = pd.concat([get_rms_queue_df(fn) for fn in fns])
        
        #df_ml[['IP', 'Lead Detector Number', 'Trail Detector Number', 'Detector Type', 'Lane', 'Speed Trap Spacing (ft)']]
        
        df_ml_lead = (df_ml[['IP', 'Lead Detector Number', 'Detector Type', 'Lane']]
            .assign(**{'Detector Type': lambda x: 'Mainline-Lead'})
            .rename(columns={'Lead Detector Number': 'Detector Number'}))
        df_ml_trail = (df_ml[['IP', 'Trail Detector Number', 'Detector Type', 'Lane']]
            .assign(**{'Detector Type': lambda x: 'Mainline-Trail'})
            .rename(columns={'Trail Detector Number': 'Detector Number'}))
        
        rms_phases = {'Mainline-Lead': 2, 
                      'Mainline-Trail': 2, 
                      'Passage': 4, 
                      'Demand': 6, 
                      'Queue': 8}
        
        rms_detectors = (pd.concat([
                    df_dem[['IP', 'Detector Number', 'Detector Type', 'Lane']],
                    df_pass[['IP','Detector Number', 'Detector Type', 'Lane']],
                    df_qu[['IP', 'Detector Number', 'Detector Type', 'Lane']],
                    df_ml_lead,
                    df_ml_trail
                ])
            .rename(columns={'Detector Number': 'Detector'})
            .drop_duplicates()
            .assign(ApproachDesc = lambda x: x['Detector Type'],
                    CallPhase = lambda x: x['Detector Type'].map(rms_phases)))
        
        rms = pd.merge(
                atspm_signals[['IPAddress','SignalID','PrimaryName','SecondaryName']], 
                rms_detectors, 
                how='outer', 
                left_on='IPAddress', 
                right_on='IP')
        rms = (rms.loc[(~rms.IP.isna()) & (~rms.SignalID.isna())]
            .drop(columns=['IPAddress'])
            .assign(SignalID=lambda x: x.SignalID.astype('int32')))
        rms = rms.rename(columns={'PrimaryName': 'Location'})
        #rms.insert(1, 'Location', rms['PrimaryName'] + ' @ ' + rms['SecondaryName'])
        rms = rms.drop(columns=['SecondaryName']) #'PrimaryName',
        
        return rms


if __name__=='__main__':


    df = reduce_ped_plans()
    df.to_csv('test.csv')
    
    """
    # --- shouldn't need this again unless detector config is updated in ATSPM    
    with engine.connect() as conn:
        det_config = pd.read_sql_table('DetectorConfig', con=conn)
    
    m = det_config.merge(veh, how='outer', indicator=True)
    n = m.groupby(['SignalID','_merge']).count()['IP'].unstack().fillna(0)
    
    s = list(set(n[(n.left_only > 0) | (n.right_only > 0)].index.values))
    m[m.SignalID.isin(s)].sort_values(['SignalID','Detector']).to_clipboard()
    
    left_signals = list(set(n[(n.both==0) & (n.right_only==0)].index.values))
    
    lefts = m[m.SignalID.isin(left_signals)].sort_values(['SignalID','Detector'])
    others = m[(~m.SignalID.isin(left_signals) & (m._merge != 'left_only'))].sort_values(['SignalID','Detector'])
    
    df = pd.concat([lefts,others]).drop(['_merge'], axis=1)
    # --- ---------------------------------------------------------------------



    
    # ------- Detector Zone Data -----------    
    fns = glob.glob(os.path.join(basepath, '*zone*.html'))
    df_zones = pd.concat([get_detector_zones_df(fn) for fn in fns])
    # Filter out Source Input == 0    
    df_zones = df_zones[df_zones['Source Input'] != '0']
    df_zones.to_csv(os.path.join(basepath, 'det_zones.csv'))
    
    
    
    # ------- MaxTime Firmware Data -----------    
    fns = glob.glob(os.path.join(basepath, '*firmware*.html'))
    df_fw = pd.concat([get_firmware_df(fn) for fn in fns]).filter(items=['IP','Firmware'])
    df_fw.to_csv(os.path.join(basepath, 'max_firmwares.csv'))


    
    # ------- MaxTime Intersection Data -----------
    fns = glob.glob(os.path.join(basepath, '*intersection*.html'))
    df_ints = pd.concat([get_intersections_df(fn) for fn in fns])
    df_ints.to_csv(os.path.join(basepath, 'max_intersections.csv'))


    
    # ------- MaxTime Ped Detector Plan Data -----------
    fns = glob.glob(os.path.join(basepath, '*ped_plan*.html'))
    df_peds = pd.concat([get_ped_plans_df(fn) for fn in fns])
    # Filter out Source Input == 0    
    df_peds = df_peds[df_peds['Call Phase'] != '0']
    df_peds.to_csv(os.path.join(basepath, 'ped_plans.csv'))

    
    
    ped = (pd.merge(left=df_peds.filter(items=['IP','Detector','Call Phase']), 
                    right=ge.filter(['IP Address','MaxView ID', 'Name']), 
                    left_on=['IP'], 
                    right_on=['IP Address'])
             .filter(items=['MaxView ID','IP', 'Name','Detector','Call Phase']))
    ped.to_csv('c:/users/alan.toppen/Code/GDOT/Ped_Phases.csv')


    # ------- MaxTime Current Time -----------
    fns = glob.glob(os.path.join(basepath, '*time.html'))
    df_time = pd.concat([get_time_df(fn) for fn in fns])
    # Filter out Source Input == 0    
    #df_time= df_peds[df_peds['Call Phase'] != '0']
    df_time.to_csv(os.path.join(basepath, 'times.csv'))
    

    # ------- MaxTime Time Zone Settings -----------
    fns = glob.glob(os.path.join(basepath, '*tz.html'))
    df_tz = pd.concat([get_tz_df(fn) for fn in fns])
    # Filter out Source Input == 0    
    #df_time= df_peds[df_peds['Call Phase'] != '0']
    df_tz.to_csv(os.path.join(basepath, 'tz.csv'))


    # ------- MaxTime Parameter Settings -----------
    fns = glob.glob(os.path.join(basepath, '*params.html'))
    df_param = pd.concat([get_params_df(fn) for fn in fns])
    # Filter out Source Input == 0    
    #df_time= df_peds[df_peds['Call Phase'] != '0']
    
    int_config = det_config.filter(items=['IP','MaxView ID','Name']).drop_duplicates()
    df_param = pd.merge(df_param, int_config, on=['IP']).filter(items=['IP','MaxView ID','Name','Extended Mode', 'StartUp Flash', 'Auto Ped Clear', 'Red Revert', 'Backup Time', 'Startup Clearance Hold Time', 'Green Flash Frequency', 'Yellow Flash Frequency', 'MCE Sequence', 'MCE Enable', 'Start Yellow Override', 'Start Red Override', 'Free Sequence', 'All Red Flash Exit Time', 'Local Flash through CVM', '3-Phase Diamond Seq', '4-Phase Diamond Seq', 'Separate Diamond Seq', 'Master By TOD', 'All Red Local Flash Sense'])
    
    df_param.to_csv(os.path.join(basepath, 'params.csv'))





    # ------- MaxTime Unit Information -----------
    basepath = 'c:/users/alan.toppen/Code/GDOT/input_points'
    fns = glob.glob(os.path.join(basepath, '*.html'))
    df_point_info = pd.concat([point_info_df(fn) for fn in fns])
    
    # ------- MaxTime Version Information -----------
    basepath = 'c:/Users/V0010894/Code/GDOT/unit_info'
    fns = glob.glob(os.path.join(basepath, '*.html'))
    df_version_info = pd.concat([get_version_df(fn) for fn in fns])
    df_version_info = df_version_info[df_version_info['Module No.']==1]




    basepath = 'c:/Users/V0010894/Code/GDOT/preempt_phasing'
    fns = glob.glob(os.path.join(basepath, '*.html'))
    df_preempt = pd.concat([get_preempt_df(fn) for fn in fns])
    df_preempt[df_preempt.Preempt.isin(['Enabled','Type'])].to_csv(os.path.join(basepath, '../preempt_phasing.csv'))


    # ------- Ramp Meter Information
    
    
    # debug stuff
    #trs = soup.find_all('tr')
    #tr = trs[30]
    #tds = tr.find_all('td')
    """    
