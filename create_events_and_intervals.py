#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#####################################
# Part of a set comprising
#   - create_events_and_intervals.py
#   - create_grand_dataframe.py
#   - query_grand_dataframe.py
#
# create_events_and_intervals.py:
# Separates 'events' and 'periods' in Uber data files, with UUIDs matching
# events to start or end limits of intervals.
# Input: files 
#   - 02 - Driver Lifetime Trips.csv
#   - 05 - Driver Online Offline.csv
#   - 08 - Driver Dispatches Offered and Accepted.csv
#   in a 'real_Uber_data/' directory (tip: symbolic link to directory)
# Output: files
#   - 02-events_df.csv
#   - 02-period_df.csv
#   - 05-events_df.csv
#   - 05-period_df.csv
#   - 08-events_df.csv
#   - 08-period_df.csv
###################################

import os
import uuid
from pathlib import Path
from typing import Callable, Optional, TypedDict

import numpy as np
import pandas as pd

pd.options.mode.chained_assignment = None  # default='warn'

SeriesMapping = Callable[[pd.Series], pd.Series]


def replace_NaN(df: pd.DataFrame, NaN_expressions: list[str]) -> pd.DataFrame:
    """Replaces all occurrences of {NaN_expressions} by {np.nan} in {df}"""
    for NaN_expression in NaN_expressions:
        df = df.replace({NaN_expression: np.nan})
    return df


def replace_mapping(
        df: pd.DataFrame,
        columns_mappings: dict[str, SeriesMapping]
) -> pd.DataFrame:
    """Applies functions in {columns_mappings} to {df} in-place"""
    df[list(columns_mappings.keys())] = df.transform(columns_mappings)
    return df


def load_data(
        file_path: Path,
        columns: Optional[dict[str, SeriesMapping]] = None,
        NaN_expressions: Optional[list[str]] = None
) -> pd.DataFrame:
    df = pd.read_csv(file_path)
    df = replace_NaN(df, (NaN_expressions or []) + ['NaN', 'NA', 'N/A', r'\N'])
    df = replace_mapping(df, columns)
    return df


def extract_1_events(
        df: pd.DataFrame,
        columns_to_merge: list[tuple[str]],
        var_name: str,
        value_names: list[str],
        label_names: list[str]
) -> pd.DataFrame:
    assert len(set([len(c) for c in columns_to_merge])) == 1, \
        f'extract_1_events: columns_to_merge must have lines of equal lengths'
    assert len(value_names) == len(columns_to_merge), \
        f'extract_1_events: value_names must have as many items as number of lines in columns_to_merge'
    assert len(label_names) == len(columns_to_merge[0]), \
        f'extract_1_events: label_names must have as many items as the number of items in each line of columns_to_merge'

    work_dfs = []
    for (column_to_merge, value_name) in zip(columns_to_merge, value_names):
        work = df.reset_index()
        work = work.melt(id_vars='index', value_vars=column_to_merge, var_name=var_name, value_name=value_name)
        work = work.rename(columns={'index': 'period_id'})
        for (column, label) in zip(column_to_merge, label_names):
            work = work.replace(column, label)
        work_dfs.append(work)

    work, *other_works = work_dfs
    for work_df in other_works:
        work = pd.merge(work, work_df, how='left', on=['period_id', 'event_type'])

    work['event_UUID'] = [uuid.uuid4() for _ in work.index]
    period_UUIDs_set = [uuid.uuid4() for _ in df.index]
    period_UUIDs = []
    for _ in label_names:
        period_UUIDs = period_UUIDs + period_UUIDs_set
    work['period_UUID'] = period_UUIDs

    return work


def extract_N_events(df: pd.DataFrame, events_df, label_names, var_names) -> pd.DataFrame:
    work = df
    for i in label_names:
        label = i + '_event_UUID'
        work[label] = events_df[events_df[var_names] == i]['event_UUID'].tolist()
    work['period_UUID'] = events_df[events_df[var_names] == label_names[0]]['period_UUID'].tolist()

    return work


def datetime64(s: pd.Series) -> pd.Series:
    return s.astype('datetime64[ns]')


def duration_mapping(shorthand: str) -> Callable[[pd.Series], pd.Series]:
    return lambda s: pd.to_timedelta(s.astype(float), unit=shorthand)


class Config(TypedDict):
    filename: str
    datetime_columns: dict[str, SeriesMapping]
    duration_columns: dict[str, SeriesMapping]
    columns_to_merge: list[tuple[str, ...]]
    var_name: str
    value_names: list[str]
    label_names: list[str]


if __name__ == '__main__':
    folder = Path(os.getcwd(), 'data')
    in_folder = Path(folder, 'raw')
    out_folder = Path(folder, 'processed')

    second = duration_mapping('s')
    minute = duration_mapping('m')

    # Luã: this was written by Emmanuel, not sure what it means
    # In a sense we have:
    # prefixes = label_names
    # suffixes = ['_timestamp_utc', '_lat', '_lng']
    # value_names = 'event' * suffixes
    # columns_to_merge = prefixes * suffixes
    # Something similar is true for the others
    # I would pivot in all three the way columns_to_merge is defined,
    # this would avoid some conceptual complexity in the double iteration on a double zip:
    # for (column_to_merge, value_name) in zip(columns_to_merge, value_names):
    #    ...
    #    for (column, label) in zip(column_to_merge, label_names):
    #        ...
    configs: list[Config] = [
        {
            'filename': '02 - Driver Lifetime Trips.csv',
            'datetime_columns': dict([(c, datetime64) for c in ['request_timestamp_local',
                                                                'request_timestamp_utc',
                                                                'begintrip_timestamp_local',
                                                                'begintrip_timestamp_utc',
                                                                'dropoff_timestamp_local',
                                                                'dropoff_timestamp_utc',
                                                                'rewindtrip_timestamp_local',
                                                                'rewindtrip_timestamp_utc']]),
            'duration_columns': {'request_to_begin_duration_seconds': second,
                                 'trip_duration_seconds': second,
                                 'fare_duration_minutes': minute,
                                 'wait_duration_minutes': minute},
            'columns_to_merge': [
                ('request_timestamp_utc', 'begintrip_timestamp_utc', 'dropoff_timestamp_utc'),
                ('request_lat', 'begintrip_lat', 'dropoff_lat'),
                ('request_lng', 'begintrip_lng', 'dropoff_lng'),
            ],
            'var_name': 'event_type',
            'value_names': ['event_timestamp_utc', 'event_lat', 'event_lng'],
            'label_names': ['request', 'begintrip', 'dropoff']
        },
        {
            'filename': '05 - Driver Online Offline.csv',
            'datetime_columns': dict([(c, datetime64) for c in ['begin_timestamp',
                                                                'end_timestamp',
                                                                'begin_timestamp_local',
                                                                'end_timestamp_local']]),
            'duration_columns': {'duration_seconds': second},
            'columns_to_merge': [
                ('begin_timestamp', 'end_timestamp'),
                ('begin_lat', 'end_lat'),
                ('begin_lng', 'end_lng')
            ],
            'var_name': 'event_type',
            'value_names': ['event_timestamp_utc', 'event_lat', 'event_lng'],
            'label_names': ['start', 'end']
        },
        {
            'filename': '08 - Driver Dispatches Offered and Accepted.csv',
            'datetime_columns': dict([(c, datetime64) for c in ['start_timestamp_utc',
                                                                'end_timestamp_utc',
                                                                'start_timestamp_local',
                                                                'end_timestamp_local']]),
            'duration_columns': {'minutes_online': minute,
                                 'minutes_active': minute,
                                 'minutes_on_trip': minute},
            'columns_to_merge': [
                ('start_timestamp_utc', 'end_timestamp_utc'),
                ('start_timestamp_local', 'end_timestamp_local'),
            ],
            'var_name': 'event_type',
            'value_names': ['event_timestamp_utc', 'event_timestamp_local'],
            'label_names': ['start', 'end'],
        }
    ]

    for config in configs:
        filename, datetime_columns, duration_columns, columns_to_merge, var_name, value_names, label_names = [
            config[k] for k in
            ['filename', 'datetime_columns', 'duration_columns', 'columns_to_merge', 'var_name', 'value_names',
             'label_names']
        ]

        filepath = Path(in_folder, filename)

        data_df = load_data(filepath, {**datetime_columns, **duration_columns})

        events_df = extract_1_events(data_df, columns_to_merge, var_name, value_names, label_names)
        period_df = extract_N_events(data_df, events_df, label_names, var_name)

        out_folder.mkdir(parents=True, exist_ok=True)
        number = filename.split(' - ')[0]
        events_df.to_csv(Path(out_folder, f'{number}-events_df.csv'), index=False)
        period_df.to_csv(Path(out_folder, f'{number}-period_df.csv'), index=False)
