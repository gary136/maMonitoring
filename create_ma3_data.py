from sqlalchemy import inspect, create_engine, Column, String, Float, Date, MetaData, Table, func
from sqlalchemy.engine import Engine
from sqlalchemy.orm.session import Session
import sys
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy.orm import sessionmaker
from database_utils import create_engine_mysql, get_or_create_table, insert_data
from collections import deque

def calculate_ma3_and_insert(engine, stock_data_table, ma3_table, session, end_date):
    data_to_insert = []
    with engine.connect() as conn:
        query = conn.execute(stock_data_table.select().order_by(stock_data_table.c.stock_code, stock_data_table.c.stock_date.desc()))
        rows = query.fetchall()

        for stock_code, group in pd.DataFrame(rows).groupby('stock_code'):
            dates = group['stock_date'].tolist()
            prices = group['closing_price'].tolist()
            date_range = deque()
            price_range = deque()
            for i in range(len(dates)):
                if i + 3 <= len(dates):
                    existing_row = session.query(ma3_table).filter(func.DATE(ma3_table.c.stock_date) == dates[i]).first()
                    if existing_row:
                        print(f"Data for {dates[0]} {stock_code} already exists in the database. Skipping insertion.")
                        continue
                    if i == 0:
                        for j in range(3):
                            date_range.append(dates[j])
                            price_range.append(prices[j])
                    else:
                        date_range.popleft()
                        date_range.append(dates[i+2])
                        price_range.popleft()
                        price_range.append(prices[i+2])
                    average_price = sum(price_range) / 3
                    data_to_insert.append({
                        'stock_date': date_range[0],
                        'stock_code': stock_code,
                        'average_price': average_price
                    })
        insert_data(engine, ma3_table, data_to_insert, session)

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

    calculate_ma3_and_insert(engine, stock_data_table, stock_data_ma3, session, end_date)

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
        end_date = datetime.now().date()

    main(end_date)