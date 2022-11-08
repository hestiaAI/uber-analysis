#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
from pathlib import PurePath

import pandas as pd

#####################################
# Part of a set comprising
#   - create_events_and_intervals.py
#   - create_grand_dataframe.py
#   - query_grand_dataframe.py
#
# query_grand_dataframe.py
# Demo script:
# returns events recorded in fusion_df.csv between two limits given in the script
# (edit script ad lib or reuse code in other applications)
###################################

pd.options.mode.chained_assignment = None  # default='warn'


# =========================================================================
def read_fusion_df(filepath):
    fusion_df = pd.read_csv(filepath)

    datetime_columns = ['request_timestamp_local', 'request_timestamp_utc',
                        'begintrip_timestamp_local', 'begintrip_timestamp_utc',
                        'dropoff_timestamp_local', 'dropoff_timestamp_utc',
                        'rewindtrip_timestamp_local', 'rewindtrip_timestamp_utc',
                        'begin_timestamp', 'end_timestamp']
    for i in datetime_columns: fusion_df[i] = pd.to_datetime(fusion_df[i])
    return fusion_df


def build_intervals_column(df, new_column_name, begin_column, end_column):
    df = df.dropna(subset=[begin_column, end_column])
    df[new_column_name] = df.apply(lambda row:
                                   pd.Interval(
                                       pd.Timestamp(row[begin_column]),
                                       pd.Timestamp(row[end_column]),
                                       closed='both'),
                                   axis=1)
    return df


def set_intervals_as_index(fusion_df):
    fusion_df = build_intervals_column(fusion_df, 'connection_intervals', 'begin_timestamp', 'end_timestamp')
    fusion_df = build_intervals_column(fusion_df, 'trip_intervals', 'request_timestamp_utc', 'dropoff_timestamp_utc')
    fusion_df = fusion_df.set_index(['connection_intervals', 'trip_intervals'])
    return fusion_df


def overlaping_intervals(df, start, end):
    start, end = pd.to_datetime(start), pd.to_datetime(end)
    query_interval = pd.Interval(start, end, closed='both')

    work_list = []
    for i, row in df.iterrows():
        if i[0].overlaps(query_interval) or i[1].overlaps(query_interval):
            work_list.append(row)
    work = pd.DataFrame(work_list)
    return work


# =========================================================================
if __name__ == '__main__':
    folder = PurePath(os.getcwd(), 'data', 'processed')

    fusion_df = read_fusion_df(PurePath(folder, 'fusion_df.csv'))
    fusion_df = set_intervals_as_index(fusion_df)

    queried_df = overlaping_intervals(fusion_df, '2017-11-30 11:22:36', '2017-11-30 12:29:02')
    print(queried_df)
