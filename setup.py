import connection
from database.silver import (calendar_dim, member_dim, holiday_dim, trading_dim, symbols_dim,
                             stock_daily_fact, member_symbol_fact)
from database.bronze import symbol_staging, stock_daily_staging
from database.gold import (moving_avg_fact, rsi_index_fact, buy_sell_moving_avg_fact,
                           member_buy_sell, gainers_losers)
import sqlalchemy as db
from sqlalchemy import text

metadata = db.MetaData()


def main():
    engine = connection.get_engine()

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
        conn.commit()

    # BRONZE LAYER SETUP

    symbol_staging.setup(engine)
    symbol_staging.populate(engine)

    stock_daily_staging.setup(engine)
    stock_daily_staging.populate(engine)

    # SILVER LAYER SETUP
    calendar_dim.setup(engine)

    member_dim.setup(engine)
    member_dim.populate(engine)

    holiday_dim.setup(engine)
    holiday_dim.populate(engine)

    trading_dim.setup(engine)
    trading_dim.populate(engine)

    symbols_dim.setup(engine)

    stock_daily_fact.setup(engine)

    member_symbol_fact.setup(engine)

    # GOLD LAYER SETUP

    moving_avg_fact.setup(engine)
    rsi_index_fact.setup(engine)
    buy_sell_moving_avg_fact.setup(engine)
    member_buy_sell.setup(engine)
    gainers_losers.setup(engine)

    inspector = db.inspect(engine)
    tables = inspector.get_table_names()
    print("Created the following tables:")
    print(tables)

    # with engine.connect() as conn:
    #     # result = conn.execute(text("SELECT schema_name FROM information_schema.schemata"))
    #     # for row in result:
    #     # print(row)
    #
    #     result = conn.execute(text("""
    #           SELECT table_schema, table_name
    #           FROM information_schema.tables
    #           WHERE table_type = 'BASE TABLE'
    #           ORDER BY table_schema, table_name;
    #       """))
    #
    #     for row in result:
    #          print(row)


if __name__ == '__main__':
    main()
