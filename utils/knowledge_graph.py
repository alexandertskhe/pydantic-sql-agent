# utils/knowledge_graph.py

import os
import json
import time
import networkx as nx
from typing import Dict, List, Optional, Any

from tools.sqlite_async import DatabaseManager, list_tables_names, describe_table

class DBKnowledgeGraph:
    """
    Knowledge Graph for representing database schema relationships
    to enhance SQL query generation.
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.graph = nx.DiGraph()
        self.cache_file = f"{os.path.splitext(db_path)[0]}_graph.json"
        self.is_initialized = False
    
    async def initialize(self):
        """Build or load the knowledge graph"""
        if os.path.exists(self.cache_file) and self._is_cache_valid():
            print(f"Loading knowledge graph from cache: {self.cache_file}")
            self._load_from_cache()
        else:
            print(f"Building knowledge graph from database: {self.db_path}")
            await self._build_from_db()
            self._save_to_cache()
        
        self.is_initialized = True
        return self
    
    async def _build_from_db(self):
        """Extract schema and build graph from database with sample data"""
        db_manager = DatabaseManager(self.db_path)
        db = await db_manager.connect()
        
        try:
            # Get tables
            tables_json = await list_tables_names(db)
            tables = json.loads(tables_json)
            print(f"Found {len(tables)} tables: {tables}")
            
            # Add table nodes
            for table_name in tables:
                print(f"Processing table: {table_name}")
                schema_json = await describe_table(db, table_name)
                schema = json.loads(schema_json)
                
                # Get sample data for the table (up to 5 rows)
                print(f"Fetching sample data for {table_name}...")
                sample_data = await self._get_sample_data(db, table_name, 5)
                print(f"Sample data rows: {len(sample_data.get('rows', []))}")
                print(f"Sample data stats keys: {list(sample_data.get('stats', {}).keys())}")
                
                # Add table node with all metadata
                self.graph.add_node(
                    table_name,
                    type="table",
                    columns=schema["columns"],
                    primary_keys=schema["primary_keys"],
                    sample_data=sample_data
                )
                
                # Verify sample data was added
                node_data = self.graph.nodes[table_name]
                print(f"Node data keys for {table_name}: {list(node_data.keys())}")
                if 'sample_data' in node_data:
                    print(f"Sample data keys: {list(node_data['sample_data'].keys())}")
                
                # Add relationship edges
                for fk in schema.get("foreign_keys", []):
                    # Add directed edge from this table to referenced table
                    self.graph.add_edge(
                        table_name,
                        fk["referenced_table"],
                        relation="references",
                        from_column=fk["column"],
                        to_column=fk["referenced_column"]
                    )
        
        finally:
            await db_manager.close()

    async def _get_sample_data(self, db, table_name, limit=5):
        """Get sample data for a table to help with query generation"""
        try:
            print(f"Starting _get_sample_data for {table_name}")
            
            # Get sample rows
            query = f"SELECT * FROM \"{table_name}\" LIMIT {limit}"
            print(f"Executing query: {query}")
            cursor = await db.execute(query)
            rows = await cursor.fetchall()
            print(f"Fetched {len(rows)} rows")
            
            # Get column names
            columns = [column[0] for column in cursor.description]
            print(f"Columns: {columns}")
            
            # Format as list of dictionaries
            sample_data = []
            for row in rows:
                row_dict = {}
                for i, value in enumerate(row):
                    # Convert None to "NULL" for better readability
                    row_dict[columns[i]] = "NULL" if value is None else value
                sample_data.append(row_dict)
            
            print(f"Processed {len(sample_data)} sample rows")
                
            # Also get unique values and ranges for important columns
            column_stats = {}
            for column in columns:
                print(f"Getting stats for column: {column}")
                try:
                    # Get distinct values count (for categorical columns)
                    count_query = f"SELECT COUNT(DISTINCT \"{column}\") FROM \"{table_name}\""
                    cursor = await db.execute(count_query)
                    distinct_count = (await cursor.fetchone())[0]
                    
                    # Get min/max (for numeric/date columns)
                    minmax_query = f"SELECT MIN(\"{column}\"), MAX(\"{column}\") FROM \"{table_name}\""
                    cursor = await db.execute(minmax_query)
                    min_val, max_val = await cursor.fetchone()
                    
                    # Get most common values (for categorical columns with few unique values)
                    common_values = []
                    if distinct_count is not None and distinct_count < 20:
                        common_values_query = f"""
                        SELECT \"{column}\", COUNT(*) as count 
                        FROM \"{table_name}\"
                        WHERE \"{column}\" IS NOT NULL
                        GROUP BY \"{column}\"
                        ORDER BY count DESC
                        LIMIT 5
                        """
                        cursor = await db.execute(common_values_query)
                        common_values_rows = await cursor.fetchall()
                        common_values = [{"value": row[0], "count": row[1]} for row in common_values_rows]
                        
                    column_stats[column] = {
                        "distinct_count": distinct_count,
                        "min_value": min_val,
                        "max_value": max_val,
                        "common_values": common_values
                    }
                    print(f"Stats for {column}: distinct={distinct_count}, min={min_val}, max={max_val}, common_values={len(common_values)}")
                    
                except Exception as e:
                    print(f"Error getting stats for column {column}: {e}")
                    # Continue with other columns even if one fails
            
            return {
                "rows": sample_data,
                "stats": column_stats
            }
            
        except Exception as e:
            print(f"Error in _get_sample_data for {table_name}: {e}")
            import traceback
            traceback.print_exc()
            return {"rows": [], "stats": {}}
    
    def _save_to_cache(self):
        """Save graph to JSON file"""
        # First ensure all node data is properly serializable
        nodes_data = {}
        for node_name in self.graph.nodes:
            node_data = dict(self.graph.nodes[node_name])
            
            # Handle sample_data specially to ensure it's serializable
            if "sample_data" in node_data:
                try:
                    # Test JSON serialization
                    json.dumps(node_data["sample_data"])
                except TypeError as e:
                    print(f"Error serializing sample_data for {node_name}: {e}")
                    # Create a clean version that's serializable
                    sample_data = node_data["sample_data"]
                    clean_sample_data = {
                        "rows": [],
                        "stats": {}
                    }
                    
                    # Process rows
                    for row in sample_data.get("rows", []):
                        clean_row = {}
                        for col, val in row.items():
                            # Convert non-serializable values to strings
                            if isinstance(val, (dict, list)):
                                clean_row[col] = json.dumps(val)
                            else:
                                try:
                                    # Test if this value is JSON serializable
                                    json.dumps(val)
                                    clean_row[col] = val
                                except (TypeError, OverflowError):
                                    clean_row[col] = str(val)
                        clean_sample_data["rows"].append(clean_row)
                    
                    # Process stats
                    for col, stats in sample_data.get("stats", {}).items():
                        clean_stats = {}
                        for stat_name, stat_val in stats.items():
                            try:
                                # Test if this value is JSON serializable
                                json.dumps(stat_val)
                                clean_stats[stat_name] = stat_val
                            except (TypeError, OverflowError):
                                clean_stats[stat_name] = str(stat_val)
                        clean_sample_data["stats"][col] = clean_stats
                    
                    node_data["sample_data"] = clean_sample_data
            
            nodes_data[node_name] = node_data
        
        data = {
            "nodes": nodes_data,
            "edges": [
                {"from": u, "to": v, **dict(self.graph.edges[u, v])}
                for u, v in self.graph.edges
            ],
            "metadata": {
                "timestamp": time.time(),
                "db_path": self.db_path
            }
        }
        
        # Verify data is serializable
        try:
            json_str = json.dumps(data)
            print(f"Cache data size: {len(json_str)} bytes")
            with open(self.cache_file, 'w') as f:
                f.write(json_str)
            print(f"Successfully saved cache to {self.cache_file}")
        except Exception as e:
            print(f"Error saving cache: {e}")
            import traceback
            traceback.print_exc()
    
    def _load_from_cache(self):
        """Load graph from JSON file"""
        with open(self.cache_file, 'r') as f:
            data = json.load(f)
        
        # Rebuild graph
        self.graph = nx.DiGraph()
        
        # Add nodes
        for node_id, attrs in data["nodes"].items():
            self.graph.add_node(node_id, **attrs)
        
        # Add edges
        for edge in data["edges"]:
            from_node = edge.pop("from")
            to_node = edge.pop("to")
            self.graph.add_edge(from_node, to_node, **edge)
    
    def _is_cache_valid(self):
        """Check if cached graph is still valid"""
        try:
            with open(self.cache_file, 'r') as f:
                data = json.load(f)
            
            # Check if our cache version has sample data
            has_sample_data = False
            for node_name, node_data in data.get("nodes", {}).items():
                if "sample_data" in node_data:
                    has_sample_data = True
                    break
            
            # If we need sample data but cache doesn't have it, invalidate cache
            if not has_sample_data:
                print("Cache doesn't have sample data, rebuilding...")
                return False
            
            # Simple check: does the cache exist and match our DB path?
            if data.get("metadata", {}).get("db_path") == self.db_path:
                return True
        except (json.JSONDecodeError, FileNotFoundError):
            pass
        
        return False
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get detailed information about a table including sample data"""
        if not self.is_initialized:
            return {"error": "Knowledge graph not initialized"}
        
        if table_name not in self.graph:
            return {"error": f"Table '{table_name}' not found in graph"}
        
        # Use dict() to make a copy of the node data
        node_data = dict(self.graph.nodes[table_name])
        print(f"Node data keys for {table_name}: {list(node_data.keys())}")
        
        # Add relationship information
        relationships = []
        for _, target, data in self.graph.out_edges(table_name, data=True):
            relationships.append({
                "target_table": target,
                "relation_type": data.get("relation", "unknown"),
                "from_column": data.get("from_column"),
                "to_column": data.get("to_column")
            })
        
        # Also check for incoming relationships
        for source, _, data in self.graph.in_edges(table_name, data=True):
            relationships.append({
                "source_table": source,
                "relation_type": data.get("relation", "unknown"),
                "from_column": data.get("from_column"),
                "to_column": data.get("to_column")
            })
        
        node_data["relationships"] = relationships
        
        # Include sample data in a more readable format
        if "sample_data" in node_data:
            print(f"Sample data keys: {list(node_data['sample_data'].keys())}")
            sample_rows = node_data["sample_data"].get("rows", [])
            sample_stats = node_data["sample_data"].get("stats", {})
            
            print(f"Sample rows: {len(sample_rows)}")
            print(f"Sample stats keys: {list(sample_stats.keys())}")
            
            # Format sample data for better readability
            formatted_samples = []
            for row in sample_rows:
                formatted_row = {}
                for col, val in row.items():
                    # Convert complex values to strings
                    if isinstance(val, (dict, list)):
                        formatted_row[col] = json.dumps(val)
                    else:
                        formatted_row[col] = val
                formatted_samples.append(formatted_row)
                
            node_data["sample_rows"] = formatted_samples
            node_data["column_statistics"] = sample_stats
        else:
            print(f"No sample_data found in node_data for {table_name}")
        
        return node_data
    
    def get_column_values(self, table_name: str, column_name: str) -> Dict[str, Any]:
        """Get sample values and statistics for a specific column"""
        if not self.is_initialized:
            return {"error": "Knowledge graph not initialized"}
        
        if table_name not in self.graph:
            return {"error": f"Table '{table_name}' not found in graph"}
        
        node_data = self.graph.nodes[table_name]
        
        # Check if column exists
        columns = [col["name"] for col in node_data.get("columns", [])]
        if column_name not in columns:
            return {"error": f"Column '{column_name}' not found in table '{table_name}'"}
        
        # Get sample data for the column
        if "sample_data" in node_data and "stats" in node_data["sample_data"]:
            column_stats = node_data["sample_data"]["stats"].get(column_name, {})
            
            # Extract sample values from rows
            sample_values = []
            for row in node_data["sample_data"].get("rows", []):
                if column_name in row:
                    value = row[column_name]
                    if value not in sample_values and value != "NULL":
                        sample_values.append(value)
                        if len(sample_values) >= 10:
                            break
            
            return {
                "column": column_name,
                "sample_values": sample_values,
                "statistics": column_stats
            }
        
        return {"error": f"No sample data available for column '{column_name}'"}

    def find_join_path(self, from_table: str, to_table: str, max_depth: int = 3) -> Optional[List[Dict]]:
        """
        Find the shortest path to join two tables.
        
        Args:
            from_table: Source table name
            to_table: Target table name
            max_depth: Maximum number of intermediate tables
            
        Returns:
            List of join specifications or None if no path found
        """
        if not self.is_initialized:
            return None
            
        if from_table not in self.graph or to_table not in self.graph:
            return None
        
        try:
            # Check for direct edge first (from either direction)
            direct_joins = []
            
            # Check if there's a direct edge from from_table to to_table
            if self.graph.has_edge(from_table, to_table):
                edge_data = self.graph.edges[from_table, to_table]
                direct_joins.append({
                    "from_table": from_table,
                    "to_table": to_table,
                    "from_column": edge_data.get("from_column"),
                    "to_column": edge_data.get("to_column")
                })
                return direct_joins
                
            # Check if there's a direct edge from to_table to from_table (reverse direction)
            if self.graph.has_edge(to_table, from_table):
                edge_data = self.graph.edges[to_table, from_table]
                direct_joins.append({
                    "from_table": to_table,
                    "to_table": from_table,
                    "from_column": edge_data.get("from_column"),
                    "to_column": edge_data.get("to_column")
                })
                return direct_joins
            
            # If no direct edge, try to find a path
            try:
                # Use networkx to find the shortest path
                path = nx.shortest_path(self.graph, from_table, to_table, cutoff=max_depth)
                
                # Build join conditions for the path
                joins = []
                for i in range(len(path)-1):
                    t1, t2 = path[i], path[i+1]
                    edge_data = self.graph.edges[t1, t2]
                    joins.append({
                        "from_table": t1,
                        "to_table": t2,
                        "from_column": edge_data.get("from_column"),
                        "to_column": edge_data.get("to_column")
                    })
                
                return joins
            except nx.NetworkXNoPath:
                # Try the reverse direction
                try:
                    path = nx.shortest_path(self.graph, to_table, from_table, cutoff=max_depth)
                    
                    # Build join conditions for the path (reversed)
                    joins = []
                    for i in range(len(path)-1):
                        t1, t2 = path[i], path[i+1]
                        edge_data = self.graph.edges[t1, t2]
                        joins.append({
                            "from_table": t1,
                            "to_table": t2,
                            "from_column": edge_data.get("from_column"),
                            "to_column": edge_data.get("to_column")
                        })
                    
                    # Reverse the joins to match original query direction
                    joins.reverse()
                    for join in joins:
                        # Swap from and to
                        join["from_table"], join["to_table"] = join["to_table"], join["from_table"]
                        join["from_column"], join["to_column"] = join["to_column"], join["from_column"]
                    
                    return joins
                except nx.NetworkXNoPath:
                    return None
        except Exception as e:
            print(f"Error finding join path: {e}")
            return None
            
    def get_query_suggestion(self, tables: List[str]) -> Optional[Dict]:
        """
        Generate a query suggestion for joining multiple tables.
        
        Args:
            tables: List of table names to join
            
        Returns:
            Dictionary with query components or None if not possible
        """
        if not self.is_initialized or len(tables) < 2:
            return None
            
        # Check if all tables exist
        for table in tables:
            if table not in self.graph:
                return None
        
        # For simplicity with two tables, determine the join directly
        if len(tables) == 2:
            path = self.find_join_path(tables[0], tables[1])
            if path:
                # Determine the actual join order based on the path
                if path[0]["from_table"] == tables[0]:
                    join_order = [tables[0], tables[1]]
                else:
                    join_order = [tables[1], tables[0]]
                    
                return {
                    "join_order": join_order,
                    "joins": path,
                    "table_columns": {
                        table: [col["name"] for col in self.graph.nodes[table].get("columns", [])]
                        for table in tables
                    }
                }
        
        # For more than two tables, use a more complex approach
        # (current implementation works for more complex cases, but we can optimize later)
                
        # Find the optimal join order
        join_order = [tables[0]]
        remaining_tables = tables[1:]
        joins = []
        
        # Greedy approach: find the next closest table
        while remaining_tables:
            current_table = join_order[-1]
            best_candidate = None
            best_path = None
            
            for candidate in remaining_tables:
                path = self.find_join_path(current_table, candidate)
                if path:
                    if best_candidate is None or len(path) < len(best_path):
                        best_candidate = candidate
                        best_path = path
                        
            # If found a directly joinable table
            if best_candidate and best_path:
                join_order.append(best_candidate)
                joins.extend(best_path)
                remaining_tables.remove(best_candidate)
            else:
                # Try to find paths between any joined table and remaining ones
                found = False
                for joined_table in join_order:
                    for candidate in remaining_tables:
                        path = self.find_join_path(joined_table, candidate)
                        if path:
                            join_order.append(candidate)
                            joins.extend(path)
                            remaining_tables.remove(candidate)
                            found = True
                            break
                    if found:
                        break
                        
                # If no path found, we can't join these tables
                if not found:
                    return None
                    
        # Generate column info for each table
        table_columns = {}
        for table in tables:
            node_data = self.graph.nodes[table]
            columns = node_data.get("columns", [])
            table_columns[table] = [col["name"] for col in columns]
            
        return {
            "join_order": join_order,
            "joins": joins,
            "table_columns": table_columns
        }
        
    def suggest_sql_query(self, tables: List[str], columns: Optional[List[str]] = None) -> Optional[str]:
        """
        Generate a suggested SQL query based on specified tables and columns.
        
        Args:
            tables: List of table names to include
            columns: Optional specific columns to select (defaults to all)
            
        Returns:
            SQL query string or None if query can't be generated
        """
        if not tables:
            return None
            
        suggestion = self.get_query_suggestion(tables)
        if not suggestion:
            return None
            
        # Generate the SELECT clause
        if not columns:
            # Select all columns from all tables with table prefixes
            select_columns = []
            for table in tables:
                table_cols = suggestion["table_columns"].get(table, [])
                select_columns.extend([f"{table}.{col}" for col in table_cols])
        else:
            # Use user-specified columns
            select_columns = columns
            
        select_clause = "SELECT " + ", ".join(select_columns)
        
        # Generate the FROM clause with JOINs
        join_order = suggestion["join_order"]
        joins = suggestion["joins"]
        
        from_clause = f"FROM {join_order[0]}"
        
        for join in joins:
            from_table = join["from_table"]
            to_table = join["to_table"]
            from_col = join["from_column"]
            to_col = join["to_column"]
            
            # Only add this join if the to_table is in our tables list
            if to_table in tables:
                from_clause += f"\nJOIN {to_table} ON {from_table}.{from_col} = {to_table}.{to_col}"
        
        # Combine the clauses
        query = f"{select_clause}\n{from_clause}"
        return query