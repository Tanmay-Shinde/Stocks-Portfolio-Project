from database import connection
from database.silver import calendar_dim, member_dim, holiday_dim, trading_dim
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



    # SILVER LAYER SETUP
    calendar_dim.setup(engine)

    member_dim.setup(engine)
    member_dim.populate(engine)

    holiday_dim.setup(engine)
    holiday_dim.populate(engine)

    trading_dim.setup(engine)
    trading_dim.populate(engine)

    inspector = db.inspect(engine)
    tables = inspector.get_table_names()
    print("Created the following tables:")
    print(tables)


if __name__ == '__main__':
    main()
