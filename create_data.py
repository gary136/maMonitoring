import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import select, inspect, create_engine, Table, Column, Integer, String, Float, Date, MetaData
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm.session import Session
import sys
from twquant import stockindc as si
from sqlalchemy.orm import sessionmaker

# Function to create SQLAlchemy engine
def create_engine_mysql() -> Engine:
    engine = create_engine('mysql+pymysql://root:@localhost/twStock')
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

# def get_or_create_table(engine, metadata) -> Table:
#     inspector = inspect(engine)
#     table_names = inspector.get_table_names()
#     if 'stock_data' in table_names:
#         print("The 'stock_data' table already exists.")
#         stock_data = Table('stock_data', metadata, autoload_with=engine)
#     else:
#         print("The 'stock_data' table does not exist. Creating a new table.")
#         stock_data = Table('stock_data', metadata,
#                            Column('stock_date', Date),
#                            Column('stock_code', String(255)),
#                            Column('closing_price', Float))
#         stock_data.create(engine)
#     return stock_data

# Function to insert data into MySQL table
def insert_data(engine: Engine, table: Table, data: list, session: Session):
    if not isinstance(engine, Engine):
        raise TypeError("Argument 'engine' must be a SQLAlchemy Engine object.")
    if not isinstance(data, list):
        raise TypeError("Argument 'data' must be a list.")
    with engine.connect() as conn:
        # conn.execute(stock_data.insert(), data)
        for item in data:
            session.execute(table.insert().values(item))

# Main function
def main(num_days):
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
        
    end_date = datetime.now().date()
    # start_date = end_date - timedelta(days=19)
    date_processed = 0
    date = datetime.now().date()
    data_to_insert = []

    while date_processed < num_days:
        api_date = date.strftime('%Y%m%d')
        print(f"Fetching data for date: {api_date}")
        # df = fetch_data(api_date)
        df = si.Price(api_date, '上市')
        
        if df is not None:
            # If data is available for the given date
            for index, row in df.iterrows():
                data_to_insert.append({
                    'stock_date': row['股價日期'].date(),
                    'stock_code': row['證券代號'],
                    'closing_price': row['收盤價']
                })
            date_processed += 1
            print("Inserting data into the database...")
            insert_data(engine, stock_data_table, data_to_insert, session)
            # top_5_rows = session.query(stock_data_table).limit(5).all()
            # for row in top_5_rows:
            #     print(row)
            data_to_insert.clear()

        date -= timedelta(days=1)
    
    print("Process completed!")
    session.commit()  # Commit the transaction to persist changes
    session.close()  # Close the session

if __name__ == "__main__":
    if len(sys.argv) > 1:
        try:
            num_days = int(sys.argv[1])
        except ValueError:
            print("Invalid input. Please provide an integer value for the number of days.")
            sys.exit(1)
    else:
        num_days = 3  # Default value

    main(num_days)
