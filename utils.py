#!/usr/bin/env python3
import pandas as pd


def save_to_excel(path: str, sheets: dict[str, pd.DataFrame]):
    with pd.ExcelWriter(path) as writer:
        for name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=name)
    print(f'saved excel at {path}')
