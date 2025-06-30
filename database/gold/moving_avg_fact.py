import pandas as pd
import sqlalchemy as db


def setup(engine):
    query = "SELECT * FROM silver.stock_daily_fact"
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)

    df.sort_values(by=['symbol_id', 'date'], inplace=True)

    df['avg_7_days'] = df['Close'].rolling(7).mean()
    df['avg_14_days'] = df['Close'].rolling(14).mean()
    df['avg_21_days'] = df['Close'].rolling(21).mean()
    df['avg_28_days'] = df['Close'].rolling(28).mean()

    df.to_sql('moving_avg_fact', con=engine, schema="gold", if_exists='replace', index=False)


def remove(engine):
    metadata = db.MetaData(schema="gold")
    sym_dim = db.Table('moving_avg_fact', metadata, autoload_with=engine)
    sym_dim.drop(engine)
