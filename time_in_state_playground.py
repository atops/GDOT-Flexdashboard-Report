
# %% Imports

import pandas as pd
import pyarrow.dataset as ds
import dask.dataframe as dd
import time


# %% Read a day's worth of wide atspm event data -- As an Arrow dataset
%%time
dataset = ds.dataset(
    's3://gdot-spm/atspm_wide/date=2022-07-12/',
    format='parquet')
table = dataset.scanner(
    columns=['SignalID', 'Timestamp', 'CoordState']
).to_table()
df = table.to_pandas().sort_values(['SignalID', 'Timestamp'])

# %% Read a day's worth of wide atspm event data -- As an pandas dataframe
%%time
df = pd.read_parquet(
    's3://gdot-spm/atspm_wide/date=2022-07-12/',
    columns=['SignalID', 'Timestamp', 'CoordState']
).sort_values(['SignalID', 'Timestamp'])

# %% Group by contiguous CoordState to get the start and end. To get duration of each.
%%time
df['CoordState'] = df['CoordState'].fillna(-1)
df['group'] = (
    (df.CoordState != df.CoordState.shift(1)) |
    (df.SignalID != df.SignalID.shift(1))
).cumsum()

# %% Get total duration in each Coordination State by Signal
%%time
group_start = df.filter(['Timestamp', 'group']).groupby(['group']).min()
groups = (
    df
    .filter(['group', 'SignalID', 'CoordState'])
    .drop_duplicates()
    .set_index('group'))

state = pd.concat([groups, group_start], axis=1)

midnight = pd.Timestamp('2022-07-12') + pd.Timedelta(1, unit='D')
state['endTimestamp'] = (
    state
    .filter(['SignalID', 'Timestamp'])
    .groupby(['SignalID'])
    .shift(-1)
    .fillna(midnight))

state['duration'] = (state['endTimestamp'] -
                     state['Timestamp']).dt.total_seconds()

state = (
    state
    .filter(['SignalID', 'CoordState', 'duration'])
    .groupby(['SignalID', 'CoordState'])
    .sum()
    .reset_index())

# %%
df = pd.read_parquet(
    's3://gdot-spm/atspm_wide/date=2022-07-12/atspm_wide_217_2022-07-12.parquet'
).sort_values(['SignalID', 'Timestamp'])
de = pd.read_parquet(
    's3://gdot-spm/detections/date=2022-07-12/de_217_2022-07-12.parquet')
dc = pd.read_feather(
    's3://gdot-spm/config/atspm_det_config_good/date=2022-07-12/ATSPM_Det_Config_Good.feather')
