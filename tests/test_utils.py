#!/usr/bin/env python3

import unittest

from utils import save_to_excel
import pandas as pd
import os

test_excel_filename = 'tests/test-excel.xlsx'

class TestExcel(unittest.TestCase):
    def setUp(self):
        if os.path.exists(test_excel_filename):
            os.remove(test_excel_filename)
    def test_save_to_excel(self):
        """
        Test that it can indeed save to excel
        """
        current_dir = os.path.dirname(__file__)
        daily = pd.read_csv(f'{current_dir}/portal_daily.csv')
        dataframes = {'daily': daily,
                      'other': pd.DataFrame({'A': [1, 2, 3]})}

        self.assertFalse(os.path.exists(test_excel_filename))
        save_to_excel(test_excel_filename, dataframes)
        self.assertTrue(os.path.exists(test_excel_filename))

if __name__ == '__main__':
    unittest.main()
