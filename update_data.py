import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import select, inspect, create_engine, Table, Column, Integer, String, Float, Date, MetaData, func
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm.session import Session
import sys
from twquant import stockindc as si
from sqlalchemy.orm import sessionmaker
from database_utils import create_engine_mysql, get_or_create_table, insert_data, fetch_and_insert_data

def main():
    print("Starting data update process...")
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
        sys.exit(1)

    # Get the latest date from the stock_data table
    latest_date_query = select(func.max(stock_data_table.c.stock_date))
    latest_date = session.execute(latest_date_query).scalar()

    if latest_date:
        print(f"Latest date in the stock_data table: {latest_date}")
    else:
        latest_date = datetime.now().date() - timedelta(days=1)
        print("No data found in the stock_data table. Setting latest date to one day before the current date.")

    current_date = datetime.now().date()
    data_to_insert = []

    while latest_date <= current_date:
        fetch_and_insert_data(latest_date, None, stock_data_table, engine, session, data_to_insert)
        latest_date += timedelta(days=1)

    print("Process completed!")
    session.commit()
    session.close()

if __name__ == "__main__":
    main()