#!/usr/bin/env python3
"""
matching trips
"""
import pandas as pd
from pathlib import Path

import sys

# import math
# import os
# import re
# from configparser import ConfigParser
from typing import Callable, Any

import swifter

from src.utils import *

swifter.set_defaults()

def date_in_interval(date, interval_begin, interval_end):
    return interval_begin <= date and date <= interval_end


def next_interval_including_date(date, intervals_generator):
    # generator must be sorted in ascending order of begin date
    try:
        while rowtuple := next(intervals_generator):
            row = rowtuple[1]
            if date_in_interval(date, row.begin, row.end):
                return row
            elif date < row.begin:
                return None
    except StopIteration:
        pass
    return None


def intervals_including_date_old(date, intervals_dataframe):
    """Generator that returns all intervals that include date."""
    ids = intervals_dataframe.sort_values(by=['begin'],
                                          ascending=True)
    for rowtuple in ids.iterrows():
        row = rowtuple[1]
        if date_in_interval(date, row.begin, row.end):
            yield row
        elif date < row.begin:
            return None


def intervals_including_date(date, intervals_dataframe):
    """Generator that returns all intervals that include date."""
    ids = intervals_dataframe.sort_values(by=['begin'],
                                          ascending=True)
    intervals_df = ids[(ids['begin'] <= date) & (date <= ids['end'])]
    return (r for (i, r) in intervals_df.iterrows())


def datlocated_rows_from_intervals(intervals_dataframe):
    """
    Generator to return rows with a single date and location.

    One row with fields begin, begin_lat, begin_lng, end, end_lat, end_lng
    is returned as two values with date, lat, lng, is_begin.
    """
    ids = intervals_dataframe
    # ids = intervals_dataframe.sort_values(by=['begin'],
    #                                       ascending=True)
    for rowtuple in ids.iterrows():
        row = rowtuple[1]
        yield {'date': row['begin'],
               'is_begin': True,
               'lat': row['begin_lat'],
               'lng': row['begin_lng'],
               'row': row
               }
        yield {'date': row['end'],
               'row': row,
               'is_begin': False,
               'lat': row['end_lat'],
               'lng': row['end_lng']}


def match_dates_to_intervals(dates_dataframe, intervals_dataframe):
    """Find the datlocs that match time intervals."""
    for datloc in datlocated_rows_from_intervals(dates_dataframe):
        date = datloc['date']
        for interval in intervals_including_date(date, intervals_dataframe):
            yield (datloc, interval)


def interval_equals(series1, series2):
    """Test for equality of begin and end dates."""
    begin_equals = series1['begin'] == series2['begin']
    return begin_equals and series1['end'] == series2['end']


def find_matching_date_locations(on_off, lifetime_trips):
    """Print trips and the locations it includes."""
    match = None
    count = 0
    for loc, trip in match_dates_to_intervals(on_off, lifetime_trips):
        count += 1
        if not match or not interval_equals(match['trip'], trip):
            if match:
            # if match and len(match['locations']) > 1:
                yield match
            match = {'trip': trip, 'locations': []}
        match['locations'].append(loc)


def print_matching_date_locations(on_off, lifetime_trips):
    for match in find_matching_date_locations(on_off, lifetime_trips):
        trip = match['trip']
        print(80 * '=')
        print(trip['begin'], trip['end'], trip['distance_km'])
        print('')
        begin_loc = False
        for loc in match['locations']:
            print(loc['date'], loc['lat'], loc['lng'], loc['is_begin'])
            if begin_loc and not loc['is_begin']:
                # end of a path
                bird = loc['row']['birdeye_distance_km_x_1.5']
                print('bird eye * 1.5 = ', bird)
            begin_loc = loc['is_begin']

        yield match

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please give a zip file path as argument")
        exit()
    zipfile_path = Path(sys.argv[1])
    if not zipfile_path.exists():
        print(f'File {zipfile_path} not found')
        exit()

    timezone = "Europe/Zurich"

    with ZipFile(zipfile_path) as zf:
        print('Zip open')
        lifetime_trips = load_lifetime_trips(zf, timezone)
        on_off = load_on_off(zf, timezone)

    trip = None
    print_matching_date_locations(on_off, lifetime_trips)
    # for oo, lt in match_dates_to_intervals(on_off, lifetime_trips):
    #     current_trip = lt
    #     if not trip == current_trip:
    #         if not trip is None:
    #             break
    #         trip = current_trip
    #         print(80 * '=')
    #         print(trip['begin'], trip['end'], trip['distance_km'])
    #         print('')

    #     print(oo['date'], oo['lat'], oo['lng'], oo['is_begin'])
