
import pandas as pd
import boto3
import sys
from s3io import *


def get_mark_keys(Bucket, date_str):

    objs = []

    response = s3.list_objects_v2(Bucket=Bucket, Prefix = 'mark')
    while 'NextContinuationToken' in response:
        # print(response['NextContinuationToken'])
        print('.', end='')
        response = s3.list_objects_v2(Bucket=Bucket, Prefix='mark',  ContinuationToken=response['NextContinuationToken'])
        contents = [content for content in response['Contents'] if (f'date={date_str}' in content['Key'] and 'cctv' not in content['Key'])]
        objs.extend(contents)

    print('')
    df = pd.DataFrame(objs)
    df[['mark', 'table', 'date_string', 'filename']] = df.Key.str.split('/', expand=True)
    df = df[['table', 'date_string', 'filename', 'LastModified', 'Size']]
    df['date_string'] = df.date_string.str.replace('date=', '')
    df['LastModified'] = pd.to_datetime(df['LastModified']).dt.tz_convert('America/New_York')
    return df.set_index(['table', 'date_string']) # .unstack(-1)


if __name__ == '__main__':

    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        date_str = pd.to_datetime('today').strftime('%Y-%m-%d')

    df = get_mark_keys(conf['bucket'], date_str)
    print(df)
    df.to_parquet('lm.parquet')
