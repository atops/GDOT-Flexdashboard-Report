"""
Created on Tue Nov 26 09:46:47 2019

@author: Anthony.Gallo
"""

import os
import io
import pandas as pd
import requests
import uuid
import polling
import random
import time  #unix
import yaml
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta  #needed for 1 year subtraction
import boto3
import json
from multiprocessing.dummy import Pool

os.environ['TZ'] = 'America/New_York'

s3 = boto3.client('s3')

pd.options.display.max_columns = 10


# run query, return hourly UDC totals for the month/corridor/zone
def get_udc_response(start_date, end_date, threshold, zone, corridor, tmcs,
                     key):
    #uri = 'http://kestrel.ritis.org:8080/{}'
    uri = 'http://pda-api.ritis.org:8080/{}'

    month = (datetime.strptime(start_date,
                               '%Y-%m-%d').date()).strftime('%b %Y')

    payload = {
        "aadtPriority": ["gdot_2018", "inrix_2019"],  # ["GDOT_2018", "INRIX"],
        "calculateAgainst":
        "AVERAGE_SPEED",  #choices are FREEFLOW or AVERAGE_SPEED
        "costs": {
            "catt.inrix.udc.commercial":  #need to enter costs for each year
            {
                "2018": 100.49,
                "2019": 100.49,
                "2020": 100.49,
                "2021": 100.49
            },
            "catt.inrix.udc.passenger": {
                "2018": 17.91,
                "2019": 17.91,
                "2020": 17.91,
                "2021": 17.91
            }
        },
        "dataSource": "vpp_here",
        "dates": [{
            "end": end_date,
            "start": start_date
        }],
        "percentCommercial":
        10,  #fallback for % trucks if the data isn't available
        "threshold":
        threshold,  #delay calculated where speed falls below threshold type by threshold mph
        "thresholdType":
        "AVERAGE",  #choices are AVERAGE (historic avg speed), REFERENCE (freeflow speed), or ABSOLUTE/NONE
        "times": [{
            "end": None,
            "start": "00:00:00.000"
        }],
        "tmcPercentMappings":
        {},  #can consider some % of TMC length in UDC calc; can leave blank
        "tmcs": tmcs,
        "useDefaultPercent":
        False,  #if true, uses the percentCommercial value for all segments instead of as a fallback
        "uuid": str(uuid.uuid1())
    }
    #----------------------------------------------------------
    try:

        response = requests.post(uri.format('jobs/udc'),
                                 params={'key': key},
                                 json=payload)
        print('response status code:', response.status_code)

        while response.status_code == 429:
            time.sleep(random.uniform(30, 60))
            print('waiting...')

        if response.status_code == 200:  # Only if successful response
            print(
                f'{datetime.now().strftime("%H:%M:%S")} Starting query for {month}, {zone}, {corridor}'
            )

            # retry at intervals of 'step' until results return (status code = 200)
            jobid = json.loads(response.content.decode('utf-8'))['id']
            while True:
                x = requests.get(uri.format('jobs/status'),
                                 params={
                                     'key': key,
                                     'jobId': jobid
                                 })
                print(f'jobid: {jobid} | status code: {x.status_code}',
                      end=' | ')
                if x.status_code == 429:  # this code means too many requests. wait longer.
                    time.sleep(random.uniform(30, 60))
                elif x.status_code == 200:
                    print(
                        f"state: {json.loads(x.content.decode('utf-8'))['state']}"
                    )
                    if json.loads(
                            x.content.decode('utf-8'))['state'] == 'SUCCEEDED':
                        break
                    else:
                        time.sleep(random.uniform(5, 10))
                else:
                    time.sleep(5)

            results = requests.get(uri.format('jobs/udc/results'),
                                   params={
                                       'key': key,
                                       'uuid': payload['uuid']
                                   })

            print(
                f'{month} {datetime.now().strftime("%H:%M:%S")}: {zone}, {corridor} results received'
            )

            strResults = results.content.decode('utf-8')
            jsResults = json.loads(strResults)

            df = pd.json_normalize(jsResults['daily_results'])
            dfDailyValues = pd.concat(
                [pd.json_normalize(x) for x in df['values']])
            dfDailyValues.insert(0, 'corridor', corridor, True)
            dfDailyValues.insert(0, 'zone', zone, True)

            print(f'{month}: {zone}, {corridor} - SUCCESS')
            return dfDailyValues

        else:
            print(
                f'{month}: {zone}, {corridor}: no results received - {response.content.decode("utf-8")}'
            )
            return pd.DataFrame()

    except Exception as e:
        print(('{}: {}, {}: Error: {}').format(month, zone, corridor, e))
        return pd.DataFrame()


def get_udc_data(start_date,
                 end_date,
                 zone_corridor_tmc_df,
                 key,
                 initial_sleep_sec=0):

    # Allow sleep time to space out requests when running in a loop
    time.sleep(initial_sleep_sec)

    # 10 mph below historic avg speed;
    # this may change for certain corridors in which case script will need to be more dynamic
    threshold = 10
    start_date_1yr = (datetime.strptime(start_date, '%Y-%m-%d').date() -
                      relativedelta(years=1)).strftime('%Y-%m-%d')
    end_date_1yr = (datetime.strptime(end_date, '%Y-%m-%d').date() -
                    relativedelta(years=1)).strftime('%Y-%m-%d')

    zcdf = zone_corridor_tmc_df.groupby(
        ['Zone', 'Corridor']).apply(lambda x: x.tmc.values.tolist())
    zcdf = zcdf.reset_index().rename(columns={0: 'tmcs'})
    zcdf = zcdf[zcdf.Zone.str.startswith(
        'Zone')]  # RTOP only. Takes too long to run otherwise.
    zcdf['threshold'] = threshold
    zcdf['key'] = key

    zcdf0 = zcdf.copy()
    zcdf0['start_date'] = start_date
    zcdf0['end_date'] = end_date
    zcdf0 = zcdf0[[
        'start_date', 'end_date', 'threshold', 'Zone', 'Corridor', 'tmcs',
        'key'
    ]]

    zcdf1 = zcdf.copy()
    zcdf1['start_date'] = start_date_1yr
    zcdf1['end_date'] = end_date_1yr
    zcdf1 = zcdf1[[
        'start_date', 'end_date', 'threshold', 'Zone', 'Corridor', 'tmcs',
        'key'
    ]]

    with Pool(2) as pool:
        results = pool.starmap_async(
            get_udc_response,
            [list(row.values()) for row in zcdf0.to_dict(orient='records')])
        pool.close()
        pool.join()
    dfs = results.get()
    df0 = pd.concat(dfs).reset_index()

    with Pool(2) as pool:
        results = pool.starmap_async(
            get_udc_response,
            [list(row.values()) for row in zcdf1.to_dict(orient='records')])
        pool.close()
        pool.join()
    dfs = results.get()
    df1 = pd.concat(dfs).reset_index()

    # dfz = pd.concat([zcdf0, zcdf1])
    # with Pool(20) as pool:
    #     results = pool.starmap_async(
    #         get_udc_response,
    #         [list(row.values()) for row in dfz.to_dict(orient='records')])
    #     pool.close()
    #     pool.join()
    # dfs = results.get()
    # dfz = pd.concat(dfs).reset_index()

    df = pd.concat([df0, df1])

    print(f'{len(df)} records')
    return df


if __name__ == '__main__':

    with open('Monthly_Report_AWS.yaml') as yaml_file:
        cred = yaml.load(yaml_file, Loader=yaml.Loader)

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    # start_date is either the given day or the first day of the month
    start_date = conf['start_date']
    if start_date == 'yesterday':
        start_date = datetime.today() - timedelta(days=1)
    start_date = (start_date - timedelta(days=(start_date.day - 1))).strftime('%Y-%m-%d')

    # end date is either the given date + 1 or today.
    # end_date is not included in the query results
    end_date = conf['end_date']
    if end_date == 'yesterday':
        end_date = datetime.today() - timedelta(days=1)
    end_date = (end_date + timedelta(days=1)).strftime('%Y-%m-%d')

    #-- manual start and end dates
    # start_date = '2021-02-01'
    # end_date = '2021-02-28'
    #---

    with io.BytesIO() as data:
        s3.download_fileobj(Bucket='gdot-spm',
                            Key='Corridor_TMCs_Latest.xlsx',
                            Fileobj=data)
        tmc_df = pd.read_excel(data)
    with io.BytesIO() as data:
        s3.download_fileobj(Bucket='gdot-spm',
                            Key='Corridors_Latest.xlsx',
                            Fileobj=data)
        corridors_zones_df = pd.read_excel(data)

    zone_corridor_df = corridors_zones_df.drop_duplicates(
        ['Zone', 'Corridor'])[['Zone', 'Corridor']]
    zone_corridor_tmc_df = pd.merge(zone_corridor_df,
                                    tmc_df[['tmc', 'Corridor']],
                                    on='Corridor')

    #--- test w/ just one zone and 2 corridors - takes a while to run
    #zone_test = 'Zone 1'
    #corridors_test = ['SR 237', 'SR 141S']
    #zone_corridor_tmc_df = zone_corridor_tmc_df[(zone_corridor_tmc_df.Zone == zone_test) & (zone_corridor_tmc_df.Corridor.isin(corridors_test))]
    #---

    try:

        udc_df = get_udc_data(start_date, end_date, zone_corridor_tmc_df,
                              cred['RITIS_KEY'], 0)

    except Exception as e:
        print('error retrieving records')
        print(e)
        udc_df = pd.DataFrame()

    if len(udc_df) > 0:
        #need to do any manipulation? Or just write to parquet and upload to S3?
        filename = f'user_delay_costs_{start_date}.parquet'
        udc_df.to_parquet(filename)
        df = udc_df.sort_values(by=['zone', 'corridor',
                                    'date'])  # 'hour_current'])

        df.to_parquet(
            f's3://gdot-spm/mark/user_delay_costs/date={start_date}/{filename}'
        )

    else:
        print('No records returned.')