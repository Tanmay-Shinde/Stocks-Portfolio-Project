import yfinance as yf
import sqlalchemy as db
import pandas as pd
from sqlalchemy import text


def setup(engine):
    metadata = db.MetaData(schema="bronze")

    stock_stage = db.Table(
        'stock_daily_stage', metadata,
        db.Column('symbol_id', db.String(50), nullable=False),
        db.Column('date', db.Date, nullable=False),
        db.Column('symbol', db.String(50), nullable=False),
        db.Column('open', db.Float, nullable=False),
        db.Column('high', db.Float, nullable=False),
        db.Column('low', db.Float, nullable=False),
        db.Column('close', db.Float, nullable=False),
        db.Column('volume', db.Float, nullable=False),
        db.Column('turn_over', db.Float, nullable=False),
        db.Column('dividends', db.Float, nullable=False),
        db.Column('stock_splits', db.Float, nullable=False),
    )
    metadata.create_all(engine)


def populate(engine):
    query1 = "SELECT symbol_id, symbol FROM silver.symbols_dim"
    query2 = "SELECT date FROM silver.trading_dim"
    with engine.connect() as conn:
        symbols = pd.read_sql(query1, conn)
        dates = pd.read_sql(query2, conn)

    # symbols.info() --> 1665 rows
    # dates.info() --> 493 rows
    # base_df must have 1665 x 493 = 820845 rows

    symbols['key'] = 1
    dates['key'] = 1
    base_df = pd.merge(symbols, dates, on='key').drop("key", axis=1)

    # base_df.info() -> 820845 rows

    tickers = symbols['symbol_id'].unique().tolist()
    start = dates['date'].min()
    end = dates['date'].max()

    valid_data = {}
    failed = []

    for ticker in tickers:
        try:
            data = yf.download(ticker, start=start, end=end, auto_adjust=False)
            if not data.empty:
                valid_data[ticker] = data
            else:
                failed.append((ticker, "Empty data"))
        except Exception as e:
            failed.append((ticker, str(e)))

    # valid_data -> 1483
    failed_lst = [fail_tup[0] for fail_tup in failed] # 182

    base_df_cleaned = base_df[~base_df['symbol_id'].isin(failed_lst)] # (1665 - 182) * 493 = 731119
    # base_df_cleaned.info() --> 731119

    all_prices = []

    for symbol_id in valid_data:
        df = valid_data[symbol_id].xs(symbol_id, axis=1, level='Ticker')
        df = df.reset_index()
        df["symbol_id"] = symbol_id
        df.rename(columns={"Date": "date"}, inplace=True)
        df.drop(columns=['Adj Close'], inplace=True)
        all_prices.append(df)

    market_data = pd.concat(all_prices)
    merged_df = pd.merge(base_df_cleaned, market_data, on=["symbol_id", "date"], how="left")

    merged_df.to_sql('stock_daily_stage', con=engine, schema="bronze", if_exists='replace', index=False)


def remove(engine):
    metadata = db.MetaData(schema="bronze")
    stock_stg = db.Table('stock_daily_stage', metadata, autoload_with=engine)
    stock_stg.drop(engine)