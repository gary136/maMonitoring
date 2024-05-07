from sqlalchemy import inspect, create_engine, Column, String, Float, Date, MetaData, Table, func
from sqlalchemy.engine import Engine
from sqlalchemy.orm.session import Session
import sys
from datetime import datetime, timedelta
from sqlalchemy.orm import sessionmaker
from database_utils import create_engine_mysql, get_or_create_table, insert_data, calculate_ma_and_insert

def main(end_date):
    print("Starting MA3 data creation process...")
    engine = create_engine_mysql()
    metadata = MetaData()
    metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    stock_data_ma3_columns = [
        Column('stock_date', Date),
        Column('stock_code', String(255)),
        Column('average_price', Float)
    ]
    stock_data_table = Table('stock_data', metadata, autoload_with=engine)
    stock_data_ma3 = get_or_create_table(engine, metadata, 'stock_data_ma3', stock_data_ma3_columns)

    calculate_ma_and_insert(datetime.now().date(), end_date, 3, engine, stock_data_table, stock_data_ma3, session)

    print("Process completed!")
    session.commit()  # Commit the transaction to persist changes
    session.close()  # Close the session

if __name__ == "__main__":
    if len(sys.argv) > 1:
        try:
            end_date = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
        except ValueError:
            print("Invalid date format. Please provide a date in YYYY-MM-DD format.")
            sys.exit(1)
    else:
        end_date = None

    main(end_date)