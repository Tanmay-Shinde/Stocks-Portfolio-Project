import sqlalchemy as db
import pandas as pd


def setup(engine):
    query1 = "SELECT member_id FROM silver.member_dim"
    query2 = "SELECT DISTINCT symbol FROM silver.stock_daily_fact"
    with engine.connect() as conn:
        df = pd.read_sql_query(query1, conn)
        df2 = pd.read_sql(query2, conn)

    # df.info()
    # df2.info()

    mem_sym_pairings = []
    batch_size1 = 15
    batch_size2 = 14
    for i in range(0, 101):
        if i <= 61:
            for j in range(0, batch_size1):
                offset = i * batch_size1
                mem_sym_pairings.append((int(df['member_id'][i]), df2['symbol'][j + offset]))
                j += 1
        else:
            for j in range(0, batch_size2):
                offset = i * batch_size2
                mem_sym_pairings.append((int(df['member_id'][i]), df2['symbol'][j + offset]))
                j += 1
        i += 1

    new_df = pd.DataFrame(mem_sym_pairings)
    new_df.rename(columns={0: 'member_id', 1: 'symbol'}, inplace=True)
    new_df.head()

    new_df.to_sql('mem_sym_fact', con=engine, schema="silver", if_exists='replace', index=False)


def remove(engine):
    metadata = db.MetaData(schema="silver")
    member = db.Table('mem_sym_fact', metadata, autoload_with=engine)
    member.drop(engine)
