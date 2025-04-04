import aiosqlite
import json


# SQLite tools for Agent to use
async def get_db(db_path):
    """Dependency that provides an async SQLite connection."""
    db = await aiosqlite.connect(db_path)
    try:
        yield db
    finally:
        await db.close()

async def list_tables_names(db) -> str:
    """Get a list of all tables in the SQLite database."""
    try:
        cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = await cursor.fetchall()
        print("[DEBUG] list_tables_names: Query result:", tables)
        return json.dumps([row[0] for row in tables])
    except Exception as e:
        return f"Error: {e}"


async def describe_table(db, table_name: str) -> str:
    """Get the schema of a specific table in the database, including primary and foreign keys."""
    try:
        # Get column information
        cursor = await db.execute(f"PRAGMA table_info({table_name});")
        columns = await cursor.fetchall()

        # Extract primary keys
        primary_keys = [col[1] for col in columns if col[5] == 1]  # Check if the column is part of the primary key
        
        # Get foreign key information
        cursor = await db.execute(f"PRAGMA foreign_key_list({table_name});")
        foreign_keys = [
            {"column": fk[3], "referenced_table": fk[2], "referenced_column": fk[4]}
            for fk in await cursor.fetchall()
        ]

        # Create schema output
        schema = {
            "columns": [{"name": col[1], "type": col[2]} for col in columns],
            "primary_keys": primary_keys,
            "foreign_keys": foreign_keys
        }

        return json.dumps(schema)

    except Exception as e:
        return f"Error: {e}"
    
async def run_sql_query(db, sql_query: str, limit: int = 10) -> str:
    """Run an SQL query and return results."""
    try:
        cursor = await db.execute(sql_query)
        rows = await cursor.fetchmany(limit) if limit else await cursor.fetchall()
        print(f'Used sql_query: {sql_query}')
        print(f'Query returned {len(rows)} rows: {rows}')
        result = [dict(zip([desc[0] for desc in cursor.description], row)) for row in rows]
        # print(f'Formatted result: {result}')
        return json.dumps(result)
    except Exception as e:
        print(f'SQL Error: {e}')
        return f"Error: {e}"
    

