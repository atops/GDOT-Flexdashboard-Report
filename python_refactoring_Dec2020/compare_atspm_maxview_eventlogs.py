# -*- coding: utf-8 -*-
"""
Created on Wed Dec 16 15:46:53 2020

@author: Alan.Toppen
"""

import os
import pandas as pd
from atspm_data import *


def get_atspmdb_s3filenames(date, bucket):
    s3prefix = f'atspmdb/date={date}'
    objs = (s3_resource
            .Bucket(bucket)
            .objects
            .filter(Prefix=s3prefix))
    s3filenames = [f's3://{bucket}/{obj.key}' for obj in objs]
    return s3filenames



atspm_files_mv = get_atspm_s3filenames('2020-11-29', 'gdot-spm')
atspm_files_db = get_atspmdb_s3filenames('2020-11-29', 'gdot-spm')

mvfiles = [os.path.basename(f) for f in atspm_files_mv]
dbfiles = [os.path.basename(f) for f in atspm_files_db]

mvset = set(mvfils)
dbset = set(dbfils)

in_both = list(mvset & dbset)
in_mv_not_db = list(mvset - dbset)
in_db_not_mv = list(dbset - mvset)

for basekey in in_both:
    dbdf = pd.read_parquet(f's3://gdot-spm/atspm/date=2020-11-29/{basekey}')\
        .drop('DeviceID', axis=1)
    mvdf = pd.read_parquet(f's3://gdot-spm/atspmdb/date=2020-11-29/{basekey}')\
        .drop('DeviceID', axis=1)
    pd.merge(dbdf, mvdf, how='outer', on=['Timestamp', 'SignalID', 'EventCode', 'EventParam'])
    