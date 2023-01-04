#!/usr/bin/env python3
import sys
# sorry I'm copy pasting from the notebook
# jupyter drives me crazy

import math
import os
import re
from configparser import ConfigParser
from typing import Callable, Any

import geopy.distance
import numpy as np
import swifter

from src.utils import *

swifter.set_defaults()

all_facets = ['duration_h', 'distance_km', 'uber_paid']
all_time_properties: dict[str, Callable[[Timestamp], Any]] = {
    'day_of_week': lambda d: d.day_name(),
    'day_type': lambda d: 'weekday' if d.weekday() < 5 else 'weekend',
    'time_of_day': lambda d: 'AM' if d.hour < 12 else 'PM',
    'night': lambda d: 'night' if d.hour <= 6 or 23 < d.hour else 'day',
    'sunday': lambda d: 'sunday' if d.day_name() == 'Sunday' else 'weekday'
}

def time_tuples_to_periods(
        table: Table['t1': Timestamp, 't2': Timestamp, 't3': Timestamp],
        columns: list[str],
        extra_info: list[Callable[[pd.Series], dict]]
) -> pd.DataFrame:
    """
    Takes a dataframe where each row has N timestamps corresponding to instants of status changes,
    and converts each row into N-1 rows of periods in the corresponding status.

    :param: table: a table having a number N > 1 of time-columns and L of entries.
    :param: columns: a list of n time-column names present in {table}.
    :param: extra_info: a list of functions taking a row of df and outputting a dictionary of additional information. Cannot have keys 'begin' and 'end'.
    :return: periods: a table having L * (N-1) entries, each with a 'begin' and 'end' timestamp and associated information as specified by additional_info.
    Usage:
    df = pd.DataFrame([{'request_ts': '3:47 PM', 'begintrip_ts': '4:00 PM', 'dropoff_ts': '4:13 PM'}])
    columns = ['request_ts', 'begintrip_ts', 'dropoff_ts']
    extra_info = [lambda r: {'status': 'P2'}, lambda r: {'status': 'P3'}]
    time_tuples_to_periods(df, columns, extra_info)
    > begin    end      status
    > 3:47 PM  4:00 PM  P2
    > 4:00 PM  4:13 PM  P3
    """
    assert len(columns) == len(
        extra_info) + 1, f'The length of additional information should correspond to the number of generated periods (N-1).'
    periods = pd.DataFrame(table.swifter.apply(
        lambda r: [{'begin': r[b], 'end': r[e], **d(r)} for b, e, d in zip(columns, columns[1:], extra_info)],
        axis=1
    ).explode().to_list())
    return periods

def load_on_off(zf: ZipFile, timezone: str, pattern: str = '*Driver Online Offline.csv',
                birdeye_coefficient: float = 1.5) -> PeriodTable:
    table = find_table(pattern, zf,
                       ['begin_timestamp_utc', 'end_timestamp_utc', 'earner_state',
                        'begin_lat', 'begin_lng', 'end_lat', 'end_lng'])
    table.rename(columns={'begin_timestamp_utc': 'begin', 'end_timestamp_utc': 'end',
                          'earner_state': 'status'}, inplace=True)
    table = table.replace({r'\N': np.nan,
                           'ontrip': 'P3', 'on trip': 'P3',
                           'enroute': 'P2', 'en route': 'P2',
                           'open': 'P1', 'offline': 'P0'})
    gps_cols = ['begin_lat', 'begin_lng', 'end_lat', 'end_lng']
    for col in gps_cols:
        table[col] = table[col].astype(float)
    for col in ['begin', 'end']:
        table[col] = pd.to_datetime(table[col], utc=True).dt.tz_convert(timezone)
    table = table.dropna()
    table['birdeye_distance_km_x_1.5'] = table.swifter.apply(
        lambda r: birdeye_coefficient * geopy.distance.geodesic((r['begin_lat'], r['begin_lng']),
                                                                (r['end_lat'], r['end_lng'])).km, axis=1)
    # table.drop(columns=gps_cols, inplace=True)
    return table.dropna()


def load_lifetime_trips(zf: ZipFile, timezone: str, pattern: str = '*Driver Lifetime Trips.csv') -> PeriodTable:
    table = find_table(pattern, zf,
                       ['request_timestamp_utc', 'begintrip_timestamp_utc', 'dropoff_timestamp_utc', 'status',
                        'request_to_begin_distance_miles', 'trip_distance_miles', 'original_fare_local'])
    table = table[table.status == 'completed'].drop(columns='status')
    table.replace({r'\N': np.nan}, inplace=True)
    for col in ['request_timestamp_utc', 'begintrip_timestamp_utc', 'dropoff_timestamp_utc']:
        table[col] = pd.to_datetime(table[col], utc=True).dt.tz_convert(timezone)
    for col in ['request_to_begin_distance_miles', 'original_fare_local']:
        table[col] = table[col].astype(float)
    table = time_tuples_to_periods(
        table,
        columns=['request_timestamp_utc', 'begintrip_timestamp_utc', 'dropoff_timestamp_utc'],
        extra_info=[
            lambda r: {'status': 'P2', 'distance_km': mile2km(r['request_to_begin_distance_miles']), 'file': r['file']},
            lambda r: {'status': 'P3', 'distance_km': mile2km(r['trip_distance_miles']), 'file': r['file'],
                       'uber_paid': r['original_fare_local']}])
    return table

def load_sar(zip_path: Path, timezone: str = 'Europe/Zurich') -> Table:
    with ZipFile(zip_path) as zf:
        print(f'Zip open')
        lifetime_trips = load_lifetime_trips(zf, timezone)
        on_off = load_on_off(zf, timezone)
    print(f'Zip closed')
    return pd.concat([lifetime_trips, on_off]).reset_index(drop=True)

def date_in_interval(date, interval_begin, interval_end):
    return interval_begin <= date and date <= interval_end

def next_trip_including_date(date, lifetime_trips_generator):
    while rowtuple := next(lifetime_trips_generator):
        row = rowtuple[1]
        if date_in_interval(date, row.begin, row.end):
            print('in interval')
            return row
        elif date < row.begin:
            print('before interval')
            return None
    return None

if __name__ == "__main__":
    file_name = sys.argv[1]
    in_path = "/home/andre/workspace/uber-data/02-method/" + file_name
    timezone = "Europe/Zurich"
    # d = load_sar(in_path, timezone)

    with ZipFile(in_path) as zf:
        print(f'Zip open')
        lifetime_trips = load_lifetime_trips(zf, timezone)
        # on_off = load_on_off(zf, timezone)

    ltrips_sorted = lifetime_trips.sort_values(by=['begin'])
    ltrips_generator = ltrips_sorted.iterrows()
    # lrow = next(ltrips_generator)[1]
    # print(lrow['begin'])

    date = pd.to_datetime('2022-11-05T00:42:00+0100')
    # on_off_sorted = on_off.sort_values(by=['begin'])
    # on_off_generator = on_off_sorted.iterrows()
    # orow = next(on_off_generator)[1]
    # print(orow['begin'])
    r = next_trip_including_date(date, ltrips_generator)
    print('next', r)
