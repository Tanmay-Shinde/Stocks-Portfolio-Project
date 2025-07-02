import sqlalchemy as db
import pandas as pd


def setup(engine):
    query1 = "SELECT * FROM bronze.stock_daily_stage"
    query2 = "SELECT * FROM silver.symbols_dim"
    with engine.connect() as conn:
        df = pd.read_sql_query(query1, conn)
        df2 = pd.read_sql(query2, conn)

    df.drop(columns=['Volume', 'turn_over', 'dividends', 'stock_splits'], inplace=True)

    df2.drop(columns=['symbol', 'name_of_company', 'date_of_listing', 'paid_up_value', 'market_lot',
                      'isin_number', 'face_value'], inplace=True)

    merged_df = pd.merge(df, df2, on='symbol_id', how='inner')

    merged_df = merged_df[['symbol_id', 'symbol', 'date', 'series', 'Open', 'High', 'Low', 'Close']]

    merged_df.to_sql(name='stock_daily_fact', con=engine, schema="silver", if_exists='replace', index=False)


def remove(engine):
    metadata = db.MetaData(schema="silver")
    sym_dim = db.Table('stock_daily_fact', metadata, autoload_with=engine)
    sym_dim.drop(engine)
