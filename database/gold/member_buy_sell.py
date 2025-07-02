import pandas as pd
import sqlalchemy as db
import datetime as dt


def setup(engine):
    query = ("SELECT * FROM gold.buy_sell_moving_avg_fact WHERE buy_signal = 1 OR sell_signal = 1")
    query2 = "SELECT * FROM silver.mem_sym_fact"
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
        df2 = pd.read_sql_query(query2, conn)

    df.drop(columns=['index', 'avg_7_days', 'avg_14_days', 'result'], inplace=True)

    merged_df = pd.merge(df, df2, on='symbol', how='inner')

    merged_df['buy_sell'] = 'other'
    merged_df.loc[merged_df['buy_signal'] == 1, 'buy_sell'] = "buy"
    merged_df.loc[merged_df['sell_signal'] == 1, 'buy_sell'] = "sell"

    merged_df.drop(columns=['buy_signal', 'sell_signal'], inplace=True)

    merged_df.rename({'Close': 'price'}, axis = 1, inplace=True)

    merged_df['qty'] = 10

    merged_df['value'] = merged_df['price'] * merged_df['qty']

    merged_df['timestamp'] = dt.datetime.now()

    # buysell_df.info()

    buysell_df = merged_df[['date', 'member_id', 'symbol', 'buy_sell', 'price', 'qty', 'value', 'timestamp']]

    buysell_df = buysell_df.sort_values(by=['symbol', 'date']).reset_index(drop=True)
    first_idx = buysell_df.groupby('symbol').head(1).index
    first_sells = buysell_df.loc[first_idx][buysell_df.loc[first_idx, 'buy_sell'] == 'sell'].index
    buysell_df = buysell_df.drop(index=first_sells).reset_index(drop=True)

    buysell_df.sort_values(by='date', inplace=True)

    buysell_df['signed_value'] = buysell_df.apply(lambda x: -x['value'] if x['buy_sell'] == 'buy' else x['value'], axis=1)
    buysell_df['balance'] = buysell_df['signed_value'].cumsum()

    buysell_df.drop(columns='signed_value', inplace=True)

    buysell_df = buysell_df[['date', 'member_id', 'symbol', 'buy_sell', 'price', 'qty', 'value', 'balance', 'timestamp']]

    buysell_df.to_sql('mem_buy_sell_fact', con=engine, schema="gold", if_exists='append', index=False)


def remove(engine):
    metadata = db.MetaData(schema="gold")
    sym_dim = db.Table('mem_buy_sell_fact', metadata, autoload_with=engine)
    sym_dim.drop(engine)
