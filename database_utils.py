from sqlalchemy import create_engine, inspect, Table, Engine, func
from sqlalchemy.orm.session import Session
from twquant import stockindc as si

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