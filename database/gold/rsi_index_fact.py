import pandas as pd
import sqlalchemy as db


def setup(engine):
    query = "SELECT * FROM silver.stock_daily_fact"

    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)

    df.drop(columns=['symbol', 'series', 'Open', 'High', 'Low'], inplace=True)

    df.sort_values(by=['symbol_id', 'date'], inplace=True)

    df['last_date_price'] = df.groupby('symbol_id')['Close'].shift(1)
    df['curr_date_price'] = df['Close']

    df['changed'] = df['curr_date_price'] - df['last_date_price']
    df['gain'] = df['changed'].apply(lambda x: x if x > 0 else 0)
    df['loss'] = df['changed'].apply(lambda x: abs(x) if x < 0 else 0)

    df['avg_gain_7'] = df.groupby('symbol_id')['gain'].transform(lambda x: x.rolling(7).mean())
    df['avg_loss_7'] = df.groupby('symbol_id')['loss'].transform(lambda x: x.rolling(7).mean())

    df['avg_gain_14'] = df.groupby('symbol_id')['gain'].transform(lambda x: x.rolling(14).mean())
    df['avg_loss_14'] = df.groupby('symbol_id')['loss'].transform(lambda x: x.rolling(14).mean())

    # df['avg_gain_21'] = df.groupby('symbol_id')['gain'].transform(lambda x: x.rolling(21).mean())
    # df['avg_loss_21'] = df.groupby('symbol_id')['loss'].transform(lambda x: x.rolling(21).mean())
    #
    # df['avg_gain_28'] = df.groupby('symbol_id')['gain'].transform(lambda x: x.rolling(28).mean())
    # df['avg_loss_28'] = df.groupby('symbol_id')['loss'].transform(lambda x: x.rolling(28).mean())

    df['rs_7'] = df['avg_gain_7'] / df['avg_loss_7']
    df['rs_14'] = df['avg_gain_14'] / df['avg_loss_14']
    # df['rs_21'] = df['avg_gain_21'] / df['avg_loss_21']
    # df['rs_28'] = df['avg_gain_28'] / df['avg_loss_28']

    df['rsi_7'] = 100 - (100 / (1 + df['rs_7']))
    df['rsi_14'] = 100 - (100 / (1 + df['rs_14']))
    # df['rsi_21'] = 100 - (100 / (1 + df['rs_21']))
    # df['rsi_28'] = 100 - (100 / (1 + df['rs_28']))

    df.to_sql('rsi_index_fact', con=engine, schema="gold", if_exists='replace', index=False)
    

def remove(engine):
    metadata = db.MetaData(schema="gold")
    sym_dim = db.Table('rsi_index_fact', metadata, autoload_with=engine)
    sym_dim.drop(engine)
