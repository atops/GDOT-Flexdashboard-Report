
from datetime import datetime, timedelta
import re
import sqlalchemy as sq
import yaml
import pandas as pd


def get_aurora_engine():
    with open('Monthly_Report_AWS.yaml') as f:
        cred = yaml.safe_load(f)
    engine = sq.create_engine(f"mysql+pymysql://{cred['RDS_USERNAME']}:{cred['RDS_PASSWORD']}@{cred['RDS_HOST']}/{cred['RDS_DATABASE']}?charset=utf8mb4")
    return engine


def get_date_from_string(x, table_regex_pattern="_dy_"):
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
            engine = get_aurora_engine()
            with engine.connect() as conn:
                tabls = pd.read_sql_query("SHOW TABLES", con=conn)['Tables_in_mark1'].values

                dy_tabls = [tabl for tabl in tabls if re.search(table_regex_pattern, tabl)]
                dy_tabls = [tabl for tabl in dy_tabls if not re.search("_[rto]", tabl)]
                x = (pd.concat([pd.read_sql_query(f"SELECT MAX(Date) AS MaxDate FROM {tabl}", con=conn) for tabl in dy_tabls])
                    .reset_index()
                    .groupby("MaxDate")
                    .count()
                    .reset_index()
                    .sort_values("MaxDate")
                    .query("index > 5")
                    .iloc[0]
                    .MaxDate
                ) + timedelta(days=1)
                x = x.strftime('%F')

    else:
        x = x.strftime('%F')
    return x

