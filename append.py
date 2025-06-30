from database.bronze import symbol_staging
import connection


def main():
    success = False
    engine = connection.get_engine()
    file_path = input("Enter file path with new symbols data: ")
    try:
        success = symbol_staging.append(engine, file_path)
    except Exception as e:
        print(e)

    if success:
        print("Successfully appended to database")
    else:
        print("Failed to append to database")


if __name__ == '__main__':
    main()