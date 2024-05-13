from sqlalchemy import select, inspect, create_engine, Column, String, Float, Date, MetaData, Table, func
from sqlalchemy.engine import Engine
from sqlalchemy.orm.session import Session
import sys
from datetime import datetime, timedelta
from sqlalchemy.orm import sessionmaker
from database_utils import create_engine_mysql, get_or_create_table, insert_data, calculate_ma_and_insert

# def main(end_date):
def main(window_size):
    if window_size == None:
        window_size = 3
    print(f"Starting MA{window_size} data creating or updating process...")
    engine = create_engine_mysql()
    metadata = MetaData()
    metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    stock_data_ma_columns = [
        Column('stock_date', Date),
        Column('stock_code', String(255)),
        Column('average_price', Float)
    ]
    stock_data = Table('stock_data', metadata, autoload_with=engine)
    stock_data_ma = get_or_create_table(engine, metadata, f'stock_data_ma{window_size}', stock_data_ma_columns)

    # Get the latest date from the stock_data table
    with engine.connect() as conn:
        max_ma_date = conn.execute(select(func.max(stock_data_ma.c.stock_date))).scalar()

        # check
        date_range = conn.execute(
            select(stock_data.c.stock_date)
            .where(stock_data.c.stock_date)
            .group_by(stock_data.c.stock_date)
            .order_by(stock_data.c.stock_date.desc())
        ).fetchall()
        # print(date_range)
        if len(date_range) < window_size:
            print("No enough data found in the stock_data table")
            return
    # Filter the date_range to include only dates greater than or equal to the max_ma_date
    if max_ma_date:
        # Find the index of max_ma_date in data_range        
        max_ma_date_index = next((i for i, x in enumerate(date_range) if x[0] == max_ma_date), None)
        date_range = date_range[:max_ma_date_index - 1 + window_size]
        # date_range = [d for d in date_range if d[0] > max_ma_date]
    # print(date_range)

    for i in range(len(date_range)):
        if i + window_size <= len(date_range):
            date, end_date = date_range[i][0], date_range[i + window_size - 1][0]
            print(f"Calculating data for {date}")
            calculate_ma_and_insert(date, end_date, window_size, engine, stock_data, stock_data_ma, session)

    print("Process completed!")
    session.commit()  # Commit the transaction to persist changes
    session.close()  # Close the session

if __name__ == "__main__":
    # main()
    if len(sys.argv) > 1:
        try:
            window_size = int(sys.argv[1])
        except ValueError:
            print("Invalid window_size format. Please provide an integer.")
            sys.exit(1)
    else:
        window_size = None
    main(window_size)