import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import select, inspect, create_engine, Table, Column, Integer, String, Float, Date, MetaData, func
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm.session import Session
import sys
from twquant import stockindc as si
from sqlalchemy.orm import sessionmaker
from database_utils import create_engine_mysql, get_or_create_table, insert_data, fetch_and_insert_data

# Main function
def main(end_date):
    print("Starting data retrieval and insertion process...")
    
    engine = create_engine_mysql()
    metadata = MetaData()
    metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    stock_data_columns = [
        Column('stock_date', Date),
        Column('stock_code', String(255)),
        Column('closing_price', Float)
    ]
    stock_data_table = get_or_create_table(engine, metadata, 'stock_data', stock_data_columns)
    # Check if stock_data_table is a valid table
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    if 'stock_data' not in table_names:
        print("The 'stock_data' is not a valid table.")
        print("Terminating the program.")
        sys.exit(1)  # Terminate the program with a non-zero exit code
        
    date_processed = 0
    date = datetime.now().date()

    if end_date is None:
        num_days = 3
        while date_processed < num_days:
            date_processed += fetch_and_insert_data(date, stock_data_table, engine, session)
            date -= timedelta(days=1)
    else:
        while date > end_date:
            fetch_and_insert_data(date, stock_data_table, engine, session)
            date -= timedelta(days=1)
    
    print("Process completed!")
    session.commit()  # Commit the transaction to persist changes
    session.close()  # Close the session

if __name__ == "__main__":
    if len(sys.argv) > 1:
        try:
            end_date = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
        except ValueError:
            print("Invalid input. Please provide an integer value for the number of days.")
            sys.exit(1)
    else:
        end_date = None
    main(end_date)
