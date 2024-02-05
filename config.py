
from datetime import datetime, timedelta
import re
import sqlalchemy as sq
import yaml
import pandas as pd
import boto3
from datetime import datetime, timedelta

def get_aurora_engine():
    with open('Monthly_Report_AWS.yaml') as f:
        cred = yaml.safe_load(f)
    engine = sq.create_engine(f"mysql+pymysql://{cred['RDS_USERNAME']}:{cred['RDS_PASSWORD']}@{cred['RDS_HOST']}/{cred['RDS_DATABASE']}?charset=utf8mb4")
    return engine


def get_date_from_string(
    x,
    s3bucket = None,
    s3prefix = None,
    table_include_regex_pattern="_dy_",
    table_exclude_regex_pattern="_outstand|_report|_resolv|_task|_tpri|_tsou|_tsub|_ttyp|_kabco|_maint|_ops|_safety|_alert|_udc|_summ"
):
    if type(x) == str:
        re_da = re.compile('\d+(?= *days ago)')
        if x == 'today':
            x = datetime.today().strftime('%F')
        elif x == 'yesterday':
            x = (datetime.today() - timedelta(days=1)).strftime('%F')
        elif re_da.search(x):
            d = int(re_da.search(x).group())
            x = (datetime.today() - timedelta(days=d)).strftime('%F')
        elif x == 'first_missing':
            if s3bucket is not None and s3prefix is not None:
                s3 = boto3.resource('s3')
                all_dates = [re.search("(?<=date\=)(\d+-\d+-\d+)", obj.key).group() for obj in s3.Bucket(s3bucket).objects.filter(Prefix=s3prefix)]
                first_missing = datetime.strptime(max(all_dates), "%Y-%m-%d") + timedelta(days=1)
                first_missing = min(first_missing, datetime.today() - timedelta(days=1))
                x = first_missing.strftime("%F")
            else:
                raise Exception("Must include arguments for s3bucket and s3prefix")
    else:
        x = x.strftime('%F')
    return x

