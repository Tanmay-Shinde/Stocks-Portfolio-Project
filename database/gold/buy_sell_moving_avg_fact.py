import pandas as pd
import sqlalchemy as db
import numpy as np


def setup(engine):
    query = "SELECT * FROM gold.moving_avg_fact"
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)

    df.drop(columns=['symbol_id', 'series', 'Open', 'High', 'Low'], inplace=True)

    df['result'] = df['avg_7_days'] - df['avg_14_days']
    df['prev_result'] = df['result'].shift(1)

    df['buy_signal'] = np.where((df['result'] > 0) & (df['prev_result'] <= 0), 1, 0)
    df['sell_signal'] = np.where((df['result'] < 0) & (df['prev_result'] >= 0), 1, 0)

    df.drop('prev_result', axis=1, inplace=True)

    df.to_sql('buy_sell_moving_avg_fact', con=engine, schema="gold", if_exists='replace')


def remove(engine):
    metadata = db.MetaData(schema="gold")
    sym_dim = db.Table('moving_avg_fact', metadata, autoload_with=engine)
    sym_dim.drop(engine)
