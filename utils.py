#!/usr/bin/env python3
from typing import Dict
import pandas as pd

def save_to_excel(path, sheets):
    with pd.ExcelWriter(path) as writer:
        for name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=name)
    print('saved ', path)
