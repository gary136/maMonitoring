import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import select, inspect, create_engine, Table, Column, Integer, String, Float, Date, MetaData, func
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm.session import Session
import sys
from twquant import stockindc as si
from sqlalchemy.orm import sessionmaker
from database_utils import create_engine_mysql, get_or_create_table, insert_data

def fetch_and_insert_data(date, end_date, stock_data_table, engine, session, data_to_insert):
    api_date = date.strftime('%Y%m%d')
    existing_row = session.query(stock_data_table).filter(func.DATE(stock_data_table.c.stock_date) == api_date).first()
    if existing_row:
        print(f"Data for {api_date} already exists in the database. Skipping insertion.")
        return 1

    print(f"Fetching data for date: {api_date}", end = ' ')
    df = si.Price(api_date, '上市')
    if df is None:
        print(f"No data available")
        return 0
    for index, row in df.iterrows():
        data_to_insert.append({
            'stock_date': row['股價日期'].date(),
            'stock_code': row['證券代號'],
            'closing_price': row['收盤價']
        })
    print("Inserting data into the database...")
    insert_data(engine, stock_data_table, data_to_insert, session)
    data_to_insert.clear()
    return 1

# Main function
def main(end_date):
    print("Starting data retrieval and insertion process...")
    
    engine = create_engine_mysql()
    metadata = MetaData()
    metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # global stock_data
    # stock_data = get_or_create_table(engine, metadata)

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
        
    # end_date = datetime.now().date()
    date_processed = 0
    date = datetime.now().date()
    data_to_insert = []

    if end_date is None:
        num_days = 3
        while date_processed < num_days:
            # api_date = date.strftime('%Y%m%d')
            # existing_row = session.query(stock_data_table).filter(func.DATE(stock_data_table.c.stock_date) == api_date).first()
            # if existing_row:
            #     print(f"Data for {api_date} already exists in the database. Skipping insertion.")
            #     date_processed += 1
            #     date -= timedelta(days=1)
            #     continue
            # print(f"Fetching data for date: {api_date}")
            # df = si.Price(api_date, '上市')            
            # if df is not None:
            #     # If data is available for the given date
            #     for index, row in df.iterrows():
            #         data_to_insert.append({
            #             'stock_date': row['股價日期'].date(),
            #             'stock_code': row['證券代號'],
            #             'closing_price': row['收盤價']
            #         })
            #     date_processed += 1
            #     print("Inserting data into the database...")
            #     insert_data(engine, stock_data_table, data_to_insert, session)
            #     data_to_insert.clear()
            # date -= timedelta(days=1)

            date_processed += fetch_and_insert_data(date, None, stock_data_table, engine, session, data_to_insert)
            date -= timedelta(days=1)
    else:
        while date > end_date:
            # api_date = date.strftime('%Y%m%d')
            # existing_row = session.query(stock_data_table).filter(func.DATE(stock_data_table.c.stock_date) == api_date).first()
            # if existing_row:
            #     print(f"Data for {api_date} already exists in the database. Skipping insertion.")
            #     date -= timedelta(days=1)
            #     continue
            # print(f"Fetching data for date: {api_date}")
            # df = si.Price(api_date, '上市')            
            # if df is not None:
            #     # If data is available for the given date
            #     for index, row in df.iterrows():
            #         data_to_insert.append({
            #             'stock_date': row['股價日期'].date(),
            #             'stock_code': row['證券代號'],
            #             'closing_price': row['收盤價']
            #         })
            #     print("Inserting data into the database...")
            #     insert_data(engine, stock_data_table, data_to_insert, session)
            #     data_to_insert.clear()
            # date -= timedelta(days=1)

            fetch_and_insert_data(date, end_date, stock_data_table, engine, session, data_to_insert)
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
