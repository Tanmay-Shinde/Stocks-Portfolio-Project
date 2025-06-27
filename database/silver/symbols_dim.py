import pandas as pd
import sqlalchemy as db


def setup(engine):
    query = "SELECT * FROM bronze.symbols_stage"

    with engine.connect() as conn:
        sym_stage = pd.read_sql(query, conn)

    sym_stage.drop(columns=['Unnamed: 8', 'YahooEquiv'], inplace=True)
    # sym_stage.columns

    sym_stage.rename(
        columns={'SYMBOL':'symbol', 'NAME OF COMPANY':'name_of_company',
                 ' SERIES':'series', ' DATE OF LISTING':'date_of_listing',
                 ' PAID UP VALUE':'paid_up_value', ' MARKET LOT':'market_lot',
                 ' ISIN NUMBER':'isin_number', ' FACE VALUE':'face_value',
                 'Yahoo_Equivalent_Code':'symbol_id'},
        inplace=True
    )

    sym_stage = sym_stage[['symbol_id', 'symbol', 'name_of_company', 'series', 'date_of_listing',
                           'paid_up_value', 'market_lot', 'isin_number', 'face_value']]

    sym_stage.dropna(subset=['symbol_id'], inplace=True)
    sym_stage.drop_duplicates(subset=['symbol_id'], inplace=True)

    sym_stage['symbol_id'] = sym_stage['symbol_id'].str.strip(",'")

    sym_stage.to_sql('symbols_dim', con=engine, schema="silver", if_exists='replace', index=False)


def append(engine, new_data):
    '''
        :param engine: sqlalchemy engine to connect to db
        :param new_data: Assumes that new data is in the same csv format as the Kaggle nse_data
    '''
    new_data.drop(columns=['Unnamed: 8', 'YahooEquiv'], inplace=True)

    new_data.rename(
        columns={'SYMBOL': 'symbol', 'NAME OF COMPANY': 'name_of_company',
                 ' SERIES': 'series', ' DATE OF LISTING': 'date_of_listing',
                 ' PAID UP VALUE': 'paid_up_value', ' MARKET LOT': 'market_lot',
                 ' ISIN NUMBER': 'isin_number', ' FACE VALUE': 'face_value',
                 'Yahoo_Equivalent_Code': 'symbol_id'},
        inplace=True
    )

    sym_stage = new_data[['symbol_id', 'symbol', 'name_of_company', 'series', 'date_of_listing',
                          'paid_up_value', 'market_lot', 'isin_number', 'face_value']]

    sym_stage.drop_na(subset=['symbol_id'], inplace=True)
    sym_stage.drop_duplicates(subset=['symbol_id'], inplace=True)



    sym_stage.to_sql('symbols_dim', con=engine, schema="silver", if_exists='append', index=False)


def remove(engine):
    metadata = db.MetaData(schema="silver")
    sym_dim = db.Table('symbols_dim', metadata, autoload_with=engine)
    sym_dim.drop(engine)