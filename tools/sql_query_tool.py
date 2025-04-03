import re
from typing import Optional, Any
import json
import traceback

from tools.sqlite_db_tool import run_sql_query

async def run_sql_query_enhanced(db, sql_query: str, kg: Optional[Any] = None, limit: Optional[int] = 10) -> str:
    """
    Run a SQL query on the database with enhanced fallback strategies for empty results.
    
    Args:
        db: Database connection
        sql_query: SQL query to run
        kg: Knowledge graph object (optional)
        limit: Maximum number of rows to return (default: 10)
    
    Returns:
        Query results in JSON format with additional helpful messages if fallback strategies were used
    """
    print('Enhanced SQL query function invoked')
    
    # Execute the original query
    result = await run_sql_query(db, sql_query, limit)
    
    # Check if the result is empty (no rows returned)
    if result == '[]':
        print("Original query returned no results, attempting fallback strategies")

        # Extract table name from query if possible
        table_match = re.search(r'FROM\s+([^\s,;]+)', sql_query, re.IGNORECASE)
        if table_match:
            table_name = table_match.group(1).strip('"`[]')
        
        # Check if query has WHERE clause with exact text matches
        if 'WHERE' in sql_query.upper() and '=' in sql_query:
            # Try a modified query with LIKE instead of equals for text comparisons
            modified_query = sql_query
            
            # Find table name to get column types
            table_match = re.search(r'FROM\s+([^\s,;]+)', sql_query, re.IGNORECASE)
            if table_match:
                table_name = table_match.group(1).strip('"`[]')
                
                # Get column info to identify text columns
                try:
                    cursor = await db.execute(f"PRAGMA table_info({table_name});")
                    columns = await cursor.fetchall()
                    text_columns = [col[1] for col in columns if 'TEXT' in col[2].upper()]
                    
                    # Extract conditions from WHERE clause
                    where_match = re.search(r'WHERE\s+(.*?)(?:ORDER BY|GROUP BY|LIMIT|$)', sql_query, re.IGNORECASE | re.DOTALL)
                    if where_match:
                        where_clause = where_match.group(1).strip()
                        conditions = re.split(r'\s+AND\s+', where_clause, flags=re.IGNORECASE)
                        
                        # Modify conditions for text columns to use LIKE
                        new_conditions = []
                        for condition in conditions:
                            # Check if this condition is on a text column with exact match
                            for col in text_columns:
                                if re.search(rf'\b{re.escape(col)}\s*=\s*[\'"]', condition, re.IGNORECASE):
                                    # Replace = with LIKE and add wildcards
                                    new_condition = re.sub(
                                        rf'(\b{re.escape(col)}\s*)=\s*([\'"])(.*?)([\'"])', 
                                        r'\1 LIKE \2%\3%\4', 
                                        condition
                                    )
                                    new_conditions.append(new_condition)
                                    break
                            else:
                                new_conditions.append(condition)
                        
                        # Replace the WHERE clause in the original query
                        new_where_clause = ' AND '.join(new_conditions)
                        modified_query = re.sub(
                            r'WHERE\s+.*?(?=ORDER BY|GROUP BY|LIMIT|$)', 
                            f'WHERE {new_where_clause} ', 
                            sql_query, 
                            flags=re.IGNORECASE | re.DOTALL
                        )
                        
                        # Run the modified query
                        print(f"Trying modified query with LIKE: {modified_query}")
                        modified_result = await run_sql_query(db, modified_query, limit)
                        
                        if modified_result != '[]':
                            return modified_result + "\n\nNote: Results found using partial matching (LIKE operator) instead of exact matches."
                except Exception as e:
                    print(f"Error in fallback query strategy: {e}")
                    print(traceback.format_exc())

        # If we have knowledge graph access, suggest potential joins
        if kg and kg.is_initialized:
            # Check for single-table query that might need joins
            if "JOIN" not in sql_query.upper():
                # Get connected tables from knowledge graph
                table_info = kg.get_table_info(table_name)
                if table_info and "relationships" in table_info and table_info["relationships"]:
                    suggestion = "No results found. Consider checking related tables with joins. This table has relationships with:\n"
                    related_tables = []
                    
                    for rel in table_info["relationships"]:
                        if "target_table" in rel:
                            related_tables.append(f"- {rel['target_table']} (via {rel['from_column']} = {rel['to_column']})")
                        elif "source_table" in rel:
                            related_tables.append(f"- {rel['source_table']} (via {rel['from_column']} = {rel['to_column']})")
                    
                    if related_tables:
                        suggestion += "\n".join(related_tables)
                        suggestion += "\n\nUse knowledge_graph_tool with action='path' to find proper join paths."
                        return suggestion

    return result