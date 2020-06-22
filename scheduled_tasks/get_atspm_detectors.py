# -*- coding: utf-8 -*-
"""
Created on Thu May 23 16:18:36 2019

@author: V0010894
"""

#from datetime import datetime, timedelta
#from multiprocessing.dummy import Pool
import os
import pandas as pd
import sqlalchemy as sq
#import re
#import pyodbc
#import time
#import itertools
#import boto3
#import yaml

# Pull in the detector-related tables from ATSPM
def get_atspm_detectors(date=None):
    """
    Returns a data frame with all the most relevant configuration data
    on Detectors from the ATSPM Database. It also selects a phase to
    assign to each detector, whether it be protected or permissive.

    Args:
        date: A date on which the configuration data is requested.
              If date is None (default), use current date or most current
              configuration.

    Returns:
        A data frame with a whole bunch of columns.
    """

    uid = os.environ['ATSPM_USERNAME']
    pwd = os.environ['ATSPM_PASSWORD']
    engine = sq.create_engine('mssql+pyodbc://{}:{}@atspm'.format(uid, pwd), pool_size=20)

    with engine.connect() as conn:

        detectiontypedetectors = pd.read_sql_table('DetectionTypeDetector', con=conn)
        detectiontypes = pd.read_sql_table('DetectionTypes', con=conn)
        lanetypes = pd.read_sql_table('LaneTypes', con=conn)
        detectionhardwares = pd.read_sql_table('DetectionHardwares', con=conn)
        movementtypes = pd.read_sql_table('MovementTypes', con=conn)
        directiontypes = pd.read_sql_table('DirectionTypes', con=conn)

        detectors = pd.read_sql_table('Detectors', con=conn)
        approaches = pd.read_sql_table("Approaches", con=conn)
        signals = pd.read_sql_table('Signals', con=conn)

    # Combine detection types tables
    detectiontypes2 = (detectiontypedetectors
                       .merge(
                           detectiontypes, on=['DetectionTypeID']
                       )
                       .drop(
                           columns=['DetectionTypeID']
                       )
                       .pivot_table(
                           index=['ID'],
                           values=['Description'],
                           aggfunc=list
                       )
                       .rename(
                           columns={'Description': 'DetectionTypeDesc'}
                       )
                       .reset_index())

    # Rename reused field names (Description, Abbreviation)
    approaches = approaches.rename(
        columns={'Description': 'ApproachDesc'}
    )
    movementtypes = movementtypes.rename(
        columns={'Description': 'MovementTypeDesc',
                 'Abbreviation': 'MovementTypeAbbr'}
    )
    lanetypes = lanetypes.rename(
        columns={'Description': 'LaneTypeDesc',
                 'Abbreviation': 'LaneTypeAbbr'}
    )
    directiontypes = directiontypes.rename(
        columns={'Description': 'DirectionTypeDesc',
                 'Abbreviation': 'DirectionTypeAbbr'}
    )

    detectionhardwares = detectionhardwares.rename(
        columns={'ID': 'DetectionHardwareID'}
    )

    # Get config for a given date, if supplied
    if date:
        detectors = detectors[(detectors['DateAdded'] <= date) & \
            ((detectors['DateDisabled'] > date) | \
            detectors['DateDisabled'].isna())]
        signals = signals[signals['Start'] <= date]

    # Drop all but the latest version
    detectors = detectors.sort_values(['DetectorID', 'DateAdded', 'ID'])\
        .drop_duplicates(['DetectorID'], keep='last')

    signals = signals.sort_values(['SignalID', 'VersionID'])\
        .drop_duplicates(['SignalID'], keep='last')

    approaches['maxVersionID'] = approaches.groupby(['SignalID'])['VersionID']\
        .transform(max)
    approaches = approaches[approaches.VersionID==approaches.maxVersionID]\
        .drop(columns=['maxVersionID'])

    # Big merge
    df = (detectors.merge(movementtypes, on=['MovementTypeID'])
          .merge(detectionhardwares, on=['DetectionHardwareID'])
          .merge(lanetypes, on=['LaneTypeID'])
          .merge(detectiontypes2, on=['ID'])
          .merge(approaches, on=['ApproachID'], how='inner')
          .merge(directiontypes, on=['DirectionTypeID'])
          .merge(signals, on=['SignalID'])
          .drop(columns=['MovementTypeID',
                         'LaneTypeID',
                         'DetectionHardwareID',
                         'ApproachID',
                         'DirectionTypeID',
                         'DisplayOrder_x',
                         'DisplayOrder_y',
                         'VersionID_x',
                         'VersionID_y',
                         'VersionActionId']))

    # DetectorID is not unique for detectors
    # Drop all but last SignalID|DetChannel combination
    # according to DateAdded
    df = df.sort_values(['SignalID','DetChannel','DateAdded']).\
        drop_duplicates(['SignalID','DetChannel'], keep='last')
    
    # Calculate time from stop bar. Assumed distance and MPH are entered
    df['TimeFromStopBar'] = df.DistanceFromStopBar/df.MPH*3600/5280
    df = df.rename(
        columns={'DetChannel': 'Detector'}
    )

    df['CallPhase'] = 0

    df.loc[df['MovementTypeAbbr'] == 'L', 'CallPhase'] = \
        df.loc[df['MovementTypeAbbr'] == 'L', 'ProtectedPhaseNumber']
    df.loc[df['MovementTypeAbbr'] == 'T', 'CallPhase'] = \
        df.loc[df['MovementTypeAbbr'] == 'T', 'ProtectedPhaseNumber']

    is_through_right_use_permissive = (df['MovementTypeAbbr'] == 'TR') & \
                                      (df['PermissivePhaseNumber'] > 0) & \
                                      (~df['PermissivePhaseNumber'].isna())
    df.loc[is_through_right_use_permissive, 'CallPhase'] = \
        df.loc[is_through_right_use_permissive, 'PermissivePhaseNumber']

    is_through_right_use_protected = (df['MovementTypeAbbr'] == 'TR') & \
                                     ((df['PermissivePhaseNumber'] == 0) | \
                                       df['PermissivePhaseNumber'].isna())
    df.loc[is_through_right_use_protected, 'CallPhase'] = \
        df.loc[is_through_right_use_protected, 'ProtectedPhaseNumber']

    df.loc[df['MovementTypeAbbr'] == 'R', 'CallPhase'] = \
        df.loc[df['MovementTypeAbbr'] == 'R', 'ProtectedPhaseNumber']
    df.loc[df['MovementTypeAbbr'] == 'TL', 'CallPhase'] = \
        df.loc[df['MovementTypeAbbr'] == 'TL', 'ProtectedPhaseNumber']

    df.DetectionTypeDesc = df.DetectionTypeDesc.astype('str')

    df['CountPriority'] = 3
    is_lane_by_lane = (df.LaneTypeDesc == 'Vehicle') & \
                   (df.DetectionTypeDesc.str.contains('Lane-by-lane Count'))
    df.loc[is_lane_by_lane, 'CountPriority'] = 2
    is_advanced_count = (df.LaneTypeDesc == 'Vehicle') & \
                     (df.DetectionTypeDesc.str.contains('Advanced Count'))
    df.loc[is_advanced_count, 'CountPriority'] = 1
    df.loc[df.LaneTypeDesc == 'Exit', 'CountPriority'] = 0

    return df
