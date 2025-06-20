from database import connection
from db_setup import calendar_dim, member
import sqlalchemy as db

metadata = db.MetaData()


def main():
    engine = connection.get_engine()
    calendar_dim.setup(engine)
    member.setup(engine)
    member.populate(engine)

    inspector = db.inspect(engine)
    tables = inspector.get_table_names()
    print("Created the following tables:")
    print(tables)


if __name__ == '__main__':
    main()
