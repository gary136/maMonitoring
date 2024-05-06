from sqlalchemy import create_engine, inspect, Table, Engine
from sqlalchemy.orm.session import Session

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