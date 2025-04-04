import aiosqlite

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