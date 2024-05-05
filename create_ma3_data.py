from sqlalchemy import inspect, create_engine, Column, String, Float, Date, MetaData, Table
from sqlalchemy.engine import Engine
from sqlalchemy.orm.session import Session
import sys
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy.orm import sessionmaker

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

def calculate_ma3_and_insert(engine, stock_data_table, ma3_table, session, end_date):
    data_to_insert = []
    with engine.connect() as conn:
        query = conn.execute(stock_data_table.select().order_by(stock_data_table.c.stock_code, stock_data_table.c.stock_date.desc()))
        rows = query.fetchall()

        for stock_code, group in pd.DataFrame(rows).groupby('stock_code'):
            dates = group['stock_date'].tolist()[:3]
            prices = group['closing_price'].tolist()[:3]
            # print(stock_code, dates, prices)
            # for i in range(len(dates)):
            if len(dates) == 3:
                average_price = sum(prices) / 3
                data_to_insert.append({
                    'stock_date': dates[0],
                    'stock_code': stock_code,
                    'average_price': average_price
                })
        # print(data_to_insert)
        insert_data(engine, ma3_table, data_to_insert, session)

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