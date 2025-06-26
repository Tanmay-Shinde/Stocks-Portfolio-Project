import sqlalchemy as db
import pandas as pd


def setup(engine):
    metadata = db.MetaData()

    trading_dim = db.Table(
        'trading_dim', metadata,
        db.Column('rank', db.Integer, nullable=False),
        db.Column('date', db.Date, nullable=False),
        db.Column('day_of_month', db.Integer, nullable=False),
        db.Column('day_of_week', db.String(10), nullable=False),
        db.Column('is_weekend', db.Integer, nullable=False),
        db.Column('week_of_year', db.Integer, nullable=False),
        db.Column('month', db.String(15), nullable=False),
        db.Column('month_number', db.Integer, nullable=False),
        db.Column('quarter', db.Integer, nullable=False),
        db.Column('year', db.Integer, nullable=False),
    )
    metadata.create_all(engine)


def populate(engine):
    # query = ("SELECT RANK() OVER (PARTITION BY year ORDER BY date) AS rank, date, day_of_month, day_of_week, is_weekend,"
    #          "week_of_year, month, month_number, quarter, year"
    #          "FROM calendar_dim "
    #          "WHERE is_weekend = 0 AND date NOT IN ("
    #          "SELECT date FROM holiday_dim)")

    query = ('SELECT * FROM calendar_dim '
             'WHERE is_weekend = 0 AND date NOT IN ('
             'SELECT date FROM holiday_dim)')

    with engine.connect() as conn:
        res = pd.read_sql_query(query, conn)

    trading_dim = pd.DataFrame(res)
    trading_dim['rank'] = trading_dim.groupby('year')["date"].rank(method='dense').astype(int)

    trading_dim = trading_dim[['rank', 'date', 'day_of_month', 'day_of_week',
                               'is_weekend', 'week_of_year', 'month', 'month_number',
                               'quarter', 'year']]

    # trading_dim.info()

    trading_dim["day_of_week"] = trading_dim["day_of_week"].astype(str)
    trading_dim["month"] = trading_dim["month"].astype(str)

    trading_dim.to_sql('trading_dim', con=engine, if_exists='replace', index=False)


def remove(engine):
    metadata = db.MetaData()
    trading_dim = db.Table('trading_dim', metadata, autoload_with=engine)
    trading_dim.drop(engine)