import datetime

import pandas as pd

Timestamp = datetime.datetime


class Table(pd.DataFrame):
    """Some Python magic to be able to type hint things like
    df: Table['begin': datetime, 'end': datetime]"""

    def __class_getitem__(cls, _):
        return list[str]


PeriodTable = Table['begin': Timestamp, 'end': Timestamp]
