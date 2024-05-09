from sqlalchemy import select, create_engine, inspect, Table, Engine, func
from sqlalchemy.orm.session import Session
from twquant import stockindc as si
import pandas as pd
from collections import deque
from datetime import datetime

username = 'root'
password = ''
host = 'localhost'
database_name = 'twStock'

def create_engine_mysql() -> Engine:
    connection_string = f'mysql+pymysql://{username}:{password}@{host}/{database_name}'
    engine = create_engine(connection_string)
    return engine

def get_or_create_table(engine, metadata, table_name, columns) -> Table:
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    if table_name in table_names:
        print(f"The '{table_name}' table already exists.")
        table = Table(table_name, metadata, autoload_with=engine)
    else:
        print(f"The '{table_name}' table does not exist. Creating a new table.")
        table = Table(table_name, metadata, *columns)
        table.create(engine)
    return table

def insert_data(engine: Engine, table: Table, data: list, session: Session):
    if not isinstance(engine, Engine):
        raise TypeError("Argument 'engine' must be a SQLAlchemy Engine object.")
    if not isinstance(data, list):
        raise TypeError("Argument 'data' must be a list.")
    with engine.connect() as conn:
        for item in data:
            session.execute(table.insert().values(item))

def fetch_and_insert_data(date, stock_data_table, engine, session):
    api_date = date.strftime('%Y%m%d')
    existing_row = session.query(stock_data_table).filter(func.DATE(stock_data_table.c.stock_date) == api_date).first()
    if existing_row:
        print(f"Data for {api_date} already exists in the database. Skipping insertion.")
        return 1

    data_to_insert = []
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

# def calculate_ma3_and_insert(engine, stock_data_table, ma3_table, session, end_date):
def calculate_ma_and_insert(latest_date, end_date, window_size, engine, stock_data_table, ma_table, session):  
    # print(latest_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), window_size, engine, stock_data_table, ma_table, session)
    existing_row = session.query(ma_table).filter(
        func.DATE(ma_table.c.stock_date) == latest_date.strftime('%Y-%m-%d')
    ).first()
    if existing_row:
        print(f"Data for {latest_date} already exists in the database. Skipping insertion.")
        return

    data_to_insert = []
    with engine.connect() as conn:
        # date_range = conn.execute(select(stock_data.c.stock_date)
        #         .where(stock_data.c.stock_date <= latest_date.strftime('%Y-%m-%d'))
        #         .group_by(stock_data.c.stock_date).order_by(stock_data.c.stock_date)).fetchall()
        # # checked
        # if len(date_range) < window_size:
        #     print(f"Not enough data. Skipping insertion.")
        #     return
        query = conn.execute(stock_data_table.select()
                .where(stock_data_table.c.stock_date.between(end_date, latest_date))
                .order_by(stock_data_table.c.stock_code, stock_data_table.c.stock_date.desc()))
        # if end_date is None:
        #     query = conn.execute(stock_data_table.select()
        #             .order_by(stock_data_table.c.stock_code, stock_data_table.c.stock_date.desc()))
        # else:
        #     query = conn.execute(stock_data_table.select().where(stock_data_table.c.stock_date >= end_date)
        #             .order_by(stock_data_table.c.stock_code, stock_data_table.c.stock_date.desc()))
        rows = query.fetchall()
        # print(rows)
        for stock_code, group in pd.DataFrame(rows).groupby('stock_code'):
            dates = group['stock_date'].tolist()
            prices = group['closing_price'].tolist()
            # print(dates, prices)
            if window_size <= len(dates):
                date_range = dates[:window_size]
                price_range = prices[:window_size]
                average_price = sum(price_range) / window_size
                data_to_insert.append({
                    'stock_date': date_range[0],
                    'stock_code': stock_code,
                    'average_price': average_price
                })
        insert_data(engine, ma_table, data_to_insert, session)
    data_to_insert.clear()