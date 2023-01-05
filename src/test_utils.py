import unittest

import pandas as pd
import os

from src.utils import save_excel

test_excel_filename = 'test-excel.xlsx'


class TestExcel(unittest.TestCase):
    def setUp(self):
        if os.path.exists(test_excel_filename):
            os.remove(test_excel_filename)

    def test_save_excel(self):
        """
        Test that it can indeed save to excel
        """
        dataframes = {'daily': pd.DataFrame({'B': ['a', 'c', 'd']}),
                      'other': pd.DataFrame({'A': [1, 2, 3]})}

        self.assertFalse(os.path.exists(test_excel_filename))
        save_excel(test_excel_filename, dataframes)
        self.assertTrue(os.path.exists(test_excel_filename))
        self.setUp()


if __name__ == '__main__':
    unittest.main()
