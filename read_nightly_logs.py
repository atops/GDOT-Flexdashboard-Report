import boto3
import pandas as pd
import pytz

client = boto3.client('logs')

def get_events(start_timestamp):

    ts = int(pd.Timestamp(start_timestamp).timestamp())

    response = client.get_log_events(
        logGroupName='nightly-logs',
        logStreamName='i-0ddfe60da0c6fe4bd',
        startTime = ts)
    events = response['events']
    df_events = pd.json_normalize(events)

    df_events['timestamp'] =  (pd.to_datetime(df_events['timestamp'], unit='ms', utc=True)
                                 .dt.tz_convert(pytz.timezone('America/New_York')))
    df_events['ingestionTime'] =  (pd.to_datetime(df_events['ingestionTime'], unit='ms', utc=True)
                                     .dt.tz_convert(pytz.timezone('America/New_York')))

    return(df_events)

if __name__=='__main__':

    today = pd.to_datetime('today').date()
    df = get_events(today)
    df.to_csv('nightly_log.csv')
