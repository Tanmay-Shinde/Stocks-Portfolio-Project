import pandas as pd
from datetime import datetime
import sqlalchemy as db
from sqlalchemy import text


def setup(engine):
    start_date = '2025-01-01'
    end_date = '2025-06-30'

    date_range = pd.date_range(start_date, end_date)

    date_dim = pd.DataFrame(date_range)
    date_dim.columns = ['date']

    date_dim['day_of_month'] = date_dim.date.dt.day
    date_dim['day_of_week'] = date_dim.date.dt.day_name()
    date_dim['is_weekend'] = (date_dim['date'].dt.dayofweek >= 5).astype(int)
    date_dim['week_of_year'] = date_dim['date'].dt.isocalendar().week
    date_dim['month'] = date_dim.date.dt.month_name()
    date_dim['month_number'] = date_dim.date.dt.month
    date_dim['quarter'] = date_dim.date.dt.quarter
    date_dim['year'] = date_dim.date.dt.year

    # date_dim.columns

    date_dim.to_sql('calendar_dim', con=engine, schema="silver", index=False, if_exists='replace')


def remove(engine):
    metadata = db.MetaData(schema='silver')
    calendar_dim = db.Table('calendar_dim', metadata, autoload_with=engine)
    calendar_dim.drop(engine)
