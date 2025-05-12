import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine


def get_db_conn(drivername: str, username: str, password: str, host: str, port: str, database: str) -> Engine:
    connection_string = URL.create(
        drivername=drivername,
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
    )
    db_conn = create_engine(connection_string)
    return db_conn


def get_sql_from_file(file_name: str) -> str:
    base_dir = os.path.dirname(__file__)
    sql_path = os.path.join(base_dir, "sql", file_name)
    with open(sql_path, "r") as f:
        return f.read()


def execute_query(engine: Engine, sql: str) -> None:
    print(f"Running query {sql}")
    with engine.begin() as conn:
        conn.execute(text(sql))
    print("Query successfully executed.")


def update_url_scrape_status(db_engine: Engine, pkey: int, status: str, timestamp: str):
    sql = get_sql_from_file("update_url_scrape_status.sql")
    sql = sql.format(status=status, timestamp=timestamp, pkey=pkey)
    execute_query(db_engine, sql)


def check_table_exists(table_name: str) -> bool:
    sql = f"""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = '{table_name.lower()}'
    );
    """
    result = execute_query(sql)
    return result
