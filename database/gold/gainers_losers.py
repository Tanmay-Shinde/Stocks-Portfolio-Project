import pandas as pd
import sqlalchemy as db


def setup(engine):
    query = "SELECT * FROM silver.stock_daily_fact"
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)

    # df.columns

    df.drop(columns=['Open', 'High', 'Low'], inplace=True)

    df.rename(columns={'Close': 'close'}, inplace=True)
    df.sort_values(by=['date', 'symbol_id'], inplace=True)

    df['prev_close'] = df['close'].shift(1)

    df['result'] = df['close'] - df['prev_close']
    df.drop(columns=['close', 'prev_close'], inplace=True)

    df.dropna(inplace=True)

    df['rank'] = df.groupby('date')["result"].rank(method='dense', ascending=False).astype(int)

    df.sort_values(by=['date', 'rank'], inplace=True)
    top10_gainers = df[df['rank'] <= 10]

    df.drop(columns=['rank'], inplace=True)

    df['rank'] = df.groupby('date')["result"].rank(method='dense').astype(int)
    df.sort_values(by=['date', 'rank'], inplace=True)
    top10_losers = df[df['rank'] <= 10]

    top10_gainers.to_sql(name='top10_gainers', con=engine, schema="gold", if_exists='replace', index=False)
    top10_losers.to_sql(name='top10_losers', con=engine, schema="gold", if_exists='replace', index=False)


def remove(engine):
    metadata = db.MetaData(schema="gold")
    sym_dim = db.Table('gainers_losers', metadata, autoload_with=engine)
    sym_dim.drop(engine)
