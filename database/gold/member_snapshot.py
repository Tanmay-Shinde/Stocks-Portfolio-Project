import pandas as pd
import sqlalchemy as db


def setup(engine):
    query = ("SELECT * FROM gold.mem_buy_sell_fact")
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)

    # date, member_id, symbol, buy_sell, close_, qty,
    # invested_deducted - total investment for the transaction (+ve if 'buy', -ve if 'sell')

    # prev_remaining_qty - total no of share member held yesterday
    # remaining_qty - total no of share member holds after current transaction

    # total_buy_qty - quantity of share bought by a member for a specific stock up to the current date
    #               - need intermediate column 'buy_qty' and then sum on that by member_id, symbol
    # total_sell_qty - quantity of share sold by a member for a specific stock up to the current date
    #                - need intermediate column 'sell_qty' and then sum on that by member_id, symbol

    # current_price - total value of currently held shares calculated by latest price * remaining_qty
    # average_price - ((prev_remaining_qty * avg_price) + (qty * price)) / (prev_remaining_qty + qty))\
    #               - need intermediate column

    # current_invested - current amount invested in a stock (avg_price * remaining_qty)
    # total_investment - (prev_invested + (qty * price)) when buy_sell = 'buy' ELSE prev_invested
    # total_sell - (prev_total_sell + (qty * price)) when buy_sell = 'sell' ELSE prev_total_sell

    # profit - remaining_qty * (price - avg_price)
    # net_profit:
    #           (CASE WHEN buy_sell = 'SELL' THEN (qty * close_) + prev_total_sell
    #           ELSE prev_total_sell END
    #           + (average_price * remaining_qty)
    #           + (remaining_qty * (close_ - average_price)))
    #           -
    #           (CASE WHEN buy_sell = 'BUY' THEN (qty * close_) + prev_total_investment
    #           ELSE prev_total_investment END)
    #           AS net_profit

    # current_timestamp_co

    query = ("WITH cte1 AS (SELECT *, "
             "CASE WHEN buy_sell = 'buy' THEN qty ELSE 0 END AS buy_qty, "
             "CASE WHEN buy_sell = 'sell' THEN qty ELSE 0 END AS sell_qty, "
             "CASE WHEN buy_sell = 'buy' THEN value ELSE 0 END AS buy_value, "
             "CASE WHEN buy_sell = 'sell' THEN value ELSE 0 END AS sell_value, "
             "CASE WHEN buy_sell = 'buy' THEN (value) WHEN buy_sell = 'sell' THEN -(value) ELSE 0 "
             "END AS invested_deducted "
             "FROM gold.mem_buy_sell_fact), "
             "cte2 AS (SELECT *, "
             "SUM(buy_qty) OVER (PARTITION BY member_id, symbol ORDER BY date) AS total_buy_qty, "
             "SUM(sell_qty) OVER (PARTITION BY member_id, symbol ORDER BY date) AS total_sell_qty, "
             "SUM(buy_value) OVER (PARTITION BY member_id, symbol ORDER BY date) AS total_investment, "
             "SUM(sell_value) OVER (PARTITION BY member_id, symbol ORDER BY date) AS total_sell, "
             "SUM(buy_qty - sell_qty) OVER (PARTITION BY member_id, symbol ORDER BY date) AS remaining_qty "
             "FROM cte1), "
             "cte3 AS (SELECT *, "
             "LAG(remaining_qty) OVER (PARTITION BY member_id, symbol ORDER BY date) AS prev_remaining_qty, "
             "LAG(total_investment) OVER (PARTITION BY member_id, symbol ORDER BY date) AS prev_total_buy, "
             "LAG(total_sell) OVER (PARTITION BY member_id, symbol ORDER BY date) AS prev_total_sell "
             "FROM cte2), "
             "avg_price_cte AS (SELECT *, "
             "(remaining_qty * price) AS current_price, "
             "COALESCE(total_investment / NULLIF(total_buy_qty, 0), 0) AS average_price "
             "FROM cte3), "
             "cte4 AS (SELECT *, "
             "(average_price * remaining_qty) AS current_invested, "
             "LAG(total_investment) OVER (PARTITION BY member_id, symbol ORDER BY date) AS prev_total_investment "
             "FROM avg_price_cte) "
             "SELECT date, member_id, symbol, buy_sell, price, qty, invested_deducted, prev_remaining_qty, "
             "remaining_qty, total_buy_qty, total_sell_qty, current_price, average_price, current_invested, "
             "total_investment, total_sell, "
             "(remaining_qty * (price - average_price)) AS profit, "
             "(CASE WHEN buy_sell = 'sell' THEN (qty * price) + prev_total_sell ELSE prev_total_sell END "
             "+ (average_price * remaining_qty) + (remaining_qty * (price - average_price))) - "
             "(CASE WHEN buy_sell = 'buy' THEN (qty * price) + prev_total_investment ELSE prev_total_investment END) "
             "AS net_profit "
             "FROM cte4"
             )
    
    with engine.connect() as conn:
        df2 = pd.read_sql_query(query, conn)

    df2.to_sql('mem_snapshot', con=engine, schema="gold", if_exists='replace', index=False)


def remove(engine):
    metadata = db.MetaData(schema="gold")
    sym_dim = db.Table('mem_buy_sell_fact', metadata, autoload_with=engine)
    sym_dim.drop(engine)
