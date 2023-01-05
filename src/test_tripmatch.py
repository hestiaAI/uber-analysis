import unittest

import pandas as pd
import os

from src.tripmatch import *

def date_in_nov_22(day):
    return pd.to_datetime('2022-11-'+day)

def din(days):
    return list(map(date_in_nov_22, days))

class TestTripMatch(unittest.TestCase):
    def setUp(self):
        pass

    def test_next_interval_including_date(self):
        """
        Test find next interval that includes date
        """
        df = pd.DataFrame({
            'name': ['a', 'b','c', 'd'],
            'begin': din(['05', '09', '10', '11']),
            'end':   din(['07', '11', '13', '14'])})

        self.assertEqual('a', df.loc[0]['name'])
        dfs = df.sort_values(by=['begin'],
                                   ascending=True)
        self.assertEqual('a', dfs['name'].values[:1])

        date = pd.to_datetime('2022-11-09T00:42:00')
        r = next_interval_including_date(date, dfs.iterrows())
        self.assertEqual('b', r['name'])

        date = pd.to_datetime('2022-11-12')
        r = next_interval_including_date(date, dfs.iterrows())
        self.assertEqual('c', r['name'])

        date = pd.to_datetime('2022-11-29')
        r = next_interval_including_date(date, dfs.iterrows())
        self.assertEqual(None, r)

    def test_intervals_including_date(self):
        """
        Test find all intervals that includes date
        """
        df = pd.DataFrame({
            'name': ['a', 'b','c', 'd'],
            'begin': din(['05', '09', '10', '11']),
            'end':   din(['07', '11', '13', '14'])})

        date = pd.to_datetime('2022-11-09T00:42:00')
        rgen = intervals_including_date(date, df)
        intervals = list(rgen)
        self.assertEqual(1, len(intervals))
        self.assertEqual('b', intervals[0]['name'])

        date = pd.to_datetime('2022-11-12')
        rgen = intervals_including_date(date, df)
        intervals = list(rgen)
        self.assertEqual(2, len(intervals))
        self.assertEqual('c', intervals[0]['name'])
        self.assertEqual('d', intervals[1]['name'])

        date = pd.to_datetime('2022-11-29')
        rgen = intervals_including_date(date, df)
        intervals = list(rgen)
        self.assertEqual(0, len(intervals))

    def test_dated_rows_from_intervals(self):
        """
        Test get dated rows from intervals
        """
        df = pd.DataFrame({
            'name': ['a', 'b'],
            'begin': din(['05', '09']),
            'begin_lat': [46.177765, 46.178352],
            'begin_lng': [6.134894, 6.136019],
            'end_lat': [46.178352, 46.232922],
            'end_lng': [6.136019, 6.196911],
            'end':   din(['07', '11'])})

        rgen = dated_rows_from_intervals(df)
        rows = list(rgen)
        self.assertEqual(4, len(rows))
        self.assertEqual(4, len(rows))
        self.assertEqual('b', rows[2]['row']['name'])
        correct_dates = din(['05', '07', '09', '11'])
        dates = [r['date'] for r in rows]
        self.assertEqual(correct_dates, dates)

if __name__ == '__main__':
    unittest.main()
