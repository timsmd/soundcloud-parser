import json
import requests as req
from datetime import datetime
import pandas as pd
import pytz

def ts_from_date(date):
    if type(date) is not datetime.date:
        date = pd.to_datetime(date)
    return int(datetime(date.year, date.month, date.day, tzinfo=pytz.utc).timestamp()) * 1000


