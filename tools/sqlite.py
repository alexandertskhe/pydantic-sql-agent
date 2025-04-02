import sqlite3
import json
from typing import Optional
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

def get_db(db_path):
    """Dependency that provides a SQLite connection."""
    db_engine = create_engine(f'sqlite:///{db_path}')
    return db_engine


def list_tables_names(db_engine: Engine) -> str:
    """ Use this function to get a list of all tables in the database.
    
    Args:
        db_engine (Engine): SQLAlchemy engine object.
    
    Returns:
        str: A comma-separated list of table names.    
    """
    try:
        table_names = inspect(db_engine).get_table_names()
        return json.dumps(table_names)
    except Exception as e:
        return f"Error: {e}"
    
def describe_table(db_engine: Engine, table_name: str) -> str:
    """ Use this function to get a description of a table in the database.
    
    Args:
        db_engine (Engine): SQLAlchemy engine object.
        table_name (str): The name of the table.
    
    Returns:
        str: A description of the table.
    """
    try:
        db_inspector = inspect(db_engine)
        table_schema = db_inspector.get_columns(table_name)
        return json.dumps([str(column) for column in table_schema])
    except Exception as e:
        return f"Error getting table schema for table: {table_name}. Error: {e}"
    
def run_sql_query(db_engine: Engine, sql_query: str, limit: Optional[int] = 10) -> str:
    """ Use this function to run a SQL query on the database.
    
    Args:
        db_engine (Engine): SQLAlchemy engine object.
        sql_query (str): The SQL query to run.
        limit (Optional[int], optional): The number of rows to return. Defaults to 10.
    
    Returns:
        str: The results of the query.
    """
    with Session(db_engine) as session, session.begin():
        result = session.execute(text(sql_query))

        try:
            if limit:
                rows = result.fetchmany(limit)
            else:
                rows = result.fetchall()

            recordset = [row._asdict() for row in rows]
            print(f'Used sql_query: {sql_query}')
            return json.dumps(recordset, default=str)
        except Exception as e:
            return f"Error: {e}"
