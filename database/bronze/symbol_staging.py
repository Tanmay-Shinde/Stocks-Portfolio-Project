import pandas as pd
import sqlalchemy as db
from pathlib import Path
from database.silver import symbols_dim
from sqlalchemy import text


def setup(engine):
    metadata = db.MetaData(schema="bronze")

    symbols = db.Table(
        'symbols_stage', metadata,
        db.Column('symbol_id', db.String(50), nullable=False),
        db.Column('symbol', db.String(50), nullable=False),
        db.Column('name_of_company', db.String(50), nullable=False),
        db.Column('series', db.String(5), nullable=False),
        db.Column('date_of_listing', db.Date, nullable=False),
        db.Column('paid_up_value', db.Integer, nullable=False),
        db.Column('market_lot', db.Integer, nullable=False),
        db.Column('isin_number', db.String(50), nullable=False),
        db.Column('face_value', db.Integer, nullable=False)
    )
    metadata.create_all(engine)


def populate(engine):
    parent_dir = Path('symbol_staging.py').resolve().parent
    file_path = parent_dir / './database/raw_files/nse_data.csv'
    symbols_data = pd.read_csv(file_path)

    symbols_data.to_sql('symbols_stage', engine, schema="bronze", if_exists='replace', index=False)


def append(engine, file_path):
    '''
        :param engine: sqlalchemy engine to connect to db
        :param file_path: Assumes that new data is in the same csv format as the Kaggle nse_data
    '''
    new_data = pd.read_csv(file_path)
    new_data.to_sql('symbols_stage', engine, schema="bronze", if_exists='append', index=False)
    symbols_dim.append(engine, new_data)
    return True


def remove(engine):
    metadata = db.MetaData(schema="bronze")
    sym_stg = db.Table('symbols_stage', metadata, autoload_with=engine)
    sym_stg.drop(engine)