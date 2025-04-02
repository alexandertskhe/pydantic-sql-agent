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
        return json.dumps([dict(zip([desc[0] for desc in cursor.description], row)) for row in rows])
    except Exception as e:
        return f"Error: {e}"
    

# Database manager
class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.db = None
        
    async def connect(self):
        if self.db is None:
            self.db = await aiosqlite.connect(self.db_path)
        return self.db
        
    async def close(self):
        if self.db:
            await self.db.close()
            self.db = None

# Connection pool
# For multiple connections, implement if db connection becomes bottleneck
class DBConnectionPool:
    def __init__(self, db_path, max_connections=5):
        self.db_path = db_path
        self.max_connections = max_connections
        self.connections = []
        self.available = []
        
    async def get_connection(self):
        if self.available:
            return self.available.pop()
        
        if len(self.connections) < self.max_connections:
            conn = await aiosqlite.connect(self.db_path)
            self.connections.append(conn)
            return conn
            
        # Wait for a connection to become available
        # Implement waiting logic here
        
    async def release_connection(self, conn):
        self.available.append(conn)
        
    async def close_all(self):
        for conn in self.connections:
            await conn.close()
        self.connections = []
        self.available = []