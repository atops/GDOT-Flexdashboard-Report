
import pandas as pd
import io
import boto3

s3io_client = boto3.client('s3')


list_objects = s3io_client.list_objects

def read(FUN, Bucket, Key, **kwargs):
    with io.BytesIO() as f:
        s3io_client.download_fileobj(Bucket=Bucket, Key=Key, Fileobj=f)
        df = FUN(f, **kwargs)
    return df


def write_parquet(df, Bucket, Key):
    for col in df.dtypes[df.dtypes=='datetime64[ns]'].index.values:
        df[col] = df[col].dt.round(freq='ms') # parquet doesn't support ns timestamps
    with io.BytesIO() as f:
        df.to_parquet(f)
        s3io_client.put_object(Bucket=Bucket, Key=Key, Body=f.getvalue())

def write_feather(df, Bucket, Key):
    with io.BytesIO() as f:
        df.to_feather(f)
        s3io_client.put_object(Bucket=Bucket, Key=Key, Body=f.getvalue())

def write_excel(df, Bucket, Key):
    with io.BytesIO() as f:
        df.to_excel(f)
        s3io_client.put_object(Bucket=Bucket, Key=Key, Body=f.getvalue())

def write_csv(df, Bucket, Key):
    with io.StringIO() as f:
        df.to_csv(f)
        s3io_client.put_object(Bucket=Bucket, Key=Key, Body=f.getvalue())

def write_string(Bucket, Key, Body):
    with io.StringIO() as f:
        s3io_client.put_object(Bucket=Bucket, Key=Key, Body=Body)


def read_parquet(Bucket, Key, **kwargs):
    return read(pd.read_parquet, Bucket, Key, **kwargs)

def read_excel(Bucket, Key, **kwargs):
    return read(pd.read_excel, Bucket, Key, **kwargs)

def read_feather(Bucket, Key, **kwargs):
    return read(pd.read_feather, Bucket, Key, **kwargs)

def read_csv(Bucket, Key, **kwargs):
    with io.BytesIO() as f:
        s3io_client.download_fileobj(Bucket=Bucket, Key=Key, Fileobj=f)
        f.seek(0)
        df = pd.read_csv(f, **kwargs)
    return df


#    obj = s3.Bucket(bucket).Object(key)
#    with io.BytesIO() as f:
#        obj.download_fileobj(f)
#        rsus = pd.read_excel(f, sheet_name=0)
