import pandas as pd
import sqlalchemy as db
from pathlib import Path


def setup(engine):
    metadata = db.MetaData()

    holiday = db.Table(
        'holiday_dim', metadata,
        db.Column('holiday_name', db.String(50), nullable=False),
        db.Column('date', db.Date, nullable=False),
        db.Column('day', db.String(10), nullable=False)
    )
    metadata.create_all(engine)


def populate(engine):
    parent_dir = Path('holiday_dim.py').resolve().parent
    file_path = parent_dir / './database/raw_files/nse_holidays_data.csv'
    holidays = pd.read_csv(file_path)

    # holidays.info()
    holidays.drop(columns=['SR. NO.'], inplace=True)
    holidays.rename(columns={'Date': 'date', 'Day':'day', 'Occasion':'holiday_name'}, inplace=True)

    holidays = holidays[['holiday_name', 'date', 'day']]
    holidays['date'] = pd.to_datetime(holidays['date'], format='%Y-%m-%d')
    holidays['holiday_name'] = holidays['holiday_name'].astype(str)
    holidays['day'] = holidays['day'].astype(str)

    holidays.dropna(inplace=True)

    holidays.to_sql('holiday_dim', con=engine, index=False, if_exists='replace')

    with engine.connect() as conn:
        conn.execute("ALTER TABLE holiday_dim SET SCHEMA silver")


def remove(engine):
    metadata = db.MetaData()
    hol_dim = db.Table('holiday_dim', metadata, autoload_with=engine)
    hol_dim.drop(engine)
