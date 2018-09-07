# -*- coding: utf-8 -*-
"""
Created on Tue Aug  7 17:20:33 2018

@author: Alan.Toppen
"""

import boto3
import re

s3 = boto3.client('s3')

current_month = '2018-08'


rtop1_keys = [k['Key'] for k in s3.list_objects(Bucket='gdot-devices', Prefix='RTOP1/{}'.format(current_month))['Contents']]
rtop2_keys = [k['Key'] for k in s3.list_objects(Bucket='gdot-devices', Prefix='RTOP2/{}'.format(current_month))['Contents']]


for k in (rtop1_keys + rtop2_keys):
    if '.' in k:
        print(k)
        kl = re.sub("/\d{4}-\d{2}/\d{4} \d{2} ", "/latest/", k)
        s3.copy(CopySource={'Bucket':'gdot-devices', 'Key': k}, Bucket='gdot-devices', Key=kl)
        print(kl)
        print()

