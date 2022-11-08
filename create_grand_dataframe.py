#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
from pathlib import PurePath

import numpy as np
import pandas as pd

#####################################
# Part of a set comprising
#   - create_events_and_intervals.py
#   - create_grand_dataframe.py
#   - query_grand_dataframe.py
#
# create_grand_dataframe.py
# merges Period files to find intersections between periods events recorded
# automaticaly by the Uber application ('Driver Online Offline') and trips performed
# by the human driver ('Driver Lifetime Trips')
# Input: files
#   - 02-period_df.csv
#   - 05-period_df.csv
# Output: file
#   - fusion_df.csv
###################################

pd.options.mode.chained_assignment = None  # default='warn'


def merge_over_time_intervals2(df1, df2, start1, end1, start2, end2):
    from ncls import NCLS

    df1['start_unixts'] = df1[start1].view('int64')
    df1['end_unixts'] = df1[end1].view('int64')
    df2['start_unixts'] = df2[start2].view('int64')
    df2['end_unixts'] = df2[end2].view('int64')

    ncls = NCLS(df1['start_unixts'], df1['end_unixts'], df1.index.values)

    x1, x2 = ncls.all_overlaps_both(df2['start_unixts'].values, df2['end_unixts'].values, df2.index.values)

    df1 = df1.reindex(x2).reset_index(drop=True)
    df2 = df2.reindex(x1).reset_index(drop=True)

    df = df1.join(df2, rsuffix='2')

    df.drop(['start_unixts', 'end_unixts'], axis=1, inplace=True)
    return df


# ===== Manage NaT data ==========================================

def fix_end_timestamp(df):
    # If end_timestamp is null (NaT), replace with begin_timestamp of the next row
    df['rescue'] = df['begin_timestamp'].shift(-1)
    df['end_timestamp'] = np.where(df['end_timestamp'].isnull(),
                                   df['rescue'],
                                   df['end_timestamp'])
    df.drop(['rescue'], axis=1, inplace=True)
    return df


def fix_trips_timestamp(df, column):
    # If begintrip_timestamp_utc is null (NaT), replace with begintrip_timestamp_utc of the next row 
    df['rescue'] = df['begintrip_timestamp_utc'].shift(-1)
    df[column] = np.where(df[column].isnull(),
                          df['rescue'],
                          df[column])
    df.drop(['rescue'], axis=1, inplace=True)
    return df


# ===== loaders for known CSV files created by our tools ==========================================
def load_02(filename):
    trips_df = pd.read_csv(filename)
    datetime_columns = ['request_timestamp_local', 'request_timestamp_utc',
                        'begintrip_timestamp_local', 'begintrip_timestamp_utc',
                        'dropoff_timestamp_local', 'dropoff_timestamp_utc',
                        'rewindtrip_timestamp_local', 'rewindtrip_timestamp_utc']
    for i in datetime_columns:
        trips_df[i] = pd.to_datetime(trips_df[i])
    return trips_df


def load_05(filename):
    app_connection_full_df = pd.read_csv(filename)
    datetime_columns = ['begin_timestamp', 'end_timestamp']
    for i in datetime_columns:
        app_connection_full_df[i] = pd.to_datetime(app_connection_full_df[i])
    return app_connection_full_df


def read_grand_dataframe(filename):
    df = pd.read_csv(filename)
    datetime_columns = ['request_timestamp_local', 'request_timestamp_utc',
                        'begintrip_timestamp_local', 'begintrip_timestamp_utc',
                        'dropoff_timestamp_local', 'dropoff_timestamp_utc',
                        'rewindtrip_timestamp_local', 'rewindtrip_timestamp_utc',
                        'begin_timestamp', 'end_timestamp']
    for i in datetime_columns: df[i] = pd.to_datetime(df[i])
    return df


# ---------------
def diag(df):
    print('=== DIAGNOSTICS ===')
    print('Shape')
    print(df.shape)
    print('  Number of rows with NaN in the dataframe:',
          df.shape[0] - df.dropna(subset=['request_timestamp_local', 'request_timestamp_utc',
                                          'begintrip_timestamp_local', 'begintrip_timestamp_utc',
                                          'dropoff_timestamp_local', 'dropoff_timestamp_utc',
                                          'begin_timestamp', 'end_timestamp']).shape[0],
          # df.shape[0] - df.dropna().shape[0],
          'out of', df.shape[0]
          )
    print('       request_timestamp_local  ', df.shape[0] - df.dropna(subset=['request_timestamp_local']).shape[0])
    print('       request_timestamp_utc    ', df.shape[0] - df.dropna(subset=['request_timestamp_utc']).shape[0])
    print('       begintrip_timestamp_local', df.shape[0] - df.dropna(subset=['begintrip_timestamp_local']).shape[0])
    print('       begintrip_timestamp_utc  ', df.shape[0] - df.dropna(subset=['begintrip_timestamp_utc']).shape[0])
    print('       dropoff_timestamp_local  ', df.shape[0] - df.dropna(subset=['dropoff_timestamp_local']).shape[0])
    print('       dropoff_timestamp_utc    ', df.shape[0] - df.dropna(subset=['dropoff_timestamp_utc']).shape[0])
    print('       begin_timestamp          ', df.shape[0] - df.dropna(subset=['begin_timestamp']).shape[0])
    print('       end_timestamp            ', df.shape[0] - df.dropna(subset=['end_timestamp']).shape[0])

    print(df['request_timestamp_utc'].describe(datetime_is_numeric=True))
    print(df['begintrip_timestamp_utc'].describe(datetime_is_numeric=True))
    print(df['dropoff_timestamp_utc'].describe(datetime_is_numeric=True))
    print(df['end_timestamp'].describe(datetime_is_numeric=True))

    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(df[['begin_timestamp', 'end_timestamp']].head())
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(df[['begin_timestamp', 'end_timestamp']].tail())


if __name__ == '__main__':
    folder = PurePath(os.getcwd(), 'data', 'processed')

    app_connection_full_df = load_05(PurePath(folder, '05-period_df.csv'))
    trips_df = load_02(PurePath(folder, '02-period_df.csv'))

    app_connection_full_df = fix_end_timestamp(app_connection_full_df)
    app_connection_full_df = app_connection_full_df.dropna(subset=['begin_timestamp', 'end_timestamp'])
    trips_df = fix_trips_timestamp(trips_df, 'begintrip_timestamp_utc')
    trips_df = fix_trips_timestamp(trips_df, 'dropoff_timestamp_utc')

    fusion_df = merge_over_time_intervals2(app_connection_full_df, trips_df,
                                           'begin_timestamp', 'end_timestamp',
                                           'request_timestamp_utc', 'dropoff_timestamp_utc')

    fusion_df.to_csv(PurePath(folder, 'fusion_df.csv'), index=False)
