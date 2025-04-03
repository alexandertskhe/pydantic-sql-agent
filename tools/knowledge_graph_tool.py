from typing import Optional, List, Dict, Any
import json

# Function for extracting information about tables from the knowledge graph
async def get_table_info(kg, table_name: str) -> str:
    """
    Get detailed information about a table from the knowledge graph.
    
    Args:
        kg: Knowledge graph object
        table_name: Name of the table to get information about
        
    Returns:
        Formatted string with table information
    """
    if not kg.is_initialized:
        return "Knowledge graph is not initialized."
        
    # Get info about the table
    table_info = kg.get_table_info(table_name)
    
    # Format the response to be more readable
    result = f"### Table: {table_name}\n"
    
    # Add column information
    result += "\n#### Columns:\n"
    for col in table_info.get("columns", []):
        result += f"- {col['name']} ({col['type']})"
        if "primary_keys" in table_info and col['name'] in table_info["primary_keys"]:
            result += " (PRIMARY KEY)"
        result += "\n"
    
    # Add relationship information
    if table_info.get("relationships"):
        result += "\n#### Relationships:\n"
        for rel in table_info.get("relationships", []):
            if "target_table" in rel:
                result += f"- References {rel['target_table']} via {rel['from_column']} -> {rel['to_column']}\n"
            elif "source_table" in rel:
                result += f"- Referenced by {rel['source_table']} via {rel['from_column']} -> {rel['to_column']}\n"
    
    # Add sample data if available
    if "sample_rows" in table_info and table_info["sample_rows"]:
        result += "\n#### Sample Data (first few rows):\n"
        # Just include the first 3 rows for readability
        for i, row in enumerate(table_info["sample_rows"][:3]):
            # Format as a simple table row
            sample_row = []
            # Limit to a few key columns for readability
            important_cols = [col for col in row.keys() if col in table_info.get("primary_keys", []) 
                            or "NAME" in col or "CODE" in col or "ID" in col][:5]
            for col in important_cols:
                sample_row.append(f"{col}: {row[col]}")
            result += f"Row {i+1}: {', '.join(sample_row)}\n"
    
    return result

# Function for getting sample values for a specific column
async def get_column_samples(kg, table_name: str, column_name: str) -> str:
    """
    Get sample values and statistics for a specific column.
    
    Args:
        kg: Knowledge graph object
        table_name: Name of the table
        column_name: Name of the column
        
    Returns:
        Formatted string with column sample values and statistics
    """
    if not kg.is_initialized:
        return "Knowledge graph is not initialized."
        
    # Get sample values for the column
    column_info = kg.get_column_values(table_name, column_name)
    
    if "error" in column_info:
        return column_info["error"]
        
    result = f"### Sample values for {table_name}.{column_name}:\n"
    
    # Add sample values with more emphasis
    if "sample_values" in column_info and column_info["sample_values"]:
        result += "Sample values (use THESE EXACT VALUES in your query):\n"
        for value in column_info["sample_values"]:
            result += f"- '{value}'\n"
        result += "\n"
    
    # Add statistics
    if "statistics" in column_info:
        stats = column_info["statistics"]
        result += "Statistics:\n"
        if "distinct_count" in stats:
            result += f"- Distinct values: {stats['distinct_count']}\n"
        if "min_value" in stats and stats["min_value"] is not None:
            result += f"- Minimum value: {stats['min_value']}\n"
        if "max_value" in stats and stats["max_value"] is not None:
            result += f"- Maximum value: {stats['max_value']}\n"
        
        # Add common values if available
        if "common_values" in stats and stats["common_values"]:
            result += "- Most common values (with frequency):\n"
            for val in stats["common_values"]:
                result += f"  - '{val['value']}' (appears {val['count']} times)\n"
    
    result += "\n REMINDER: Use the EXACT values shown above in your SQL query. Do not assume or modify these values."
    
    return result

# Function for finding join paths between tables
async def find_join_path(kg, from_table: str, to_table: str) -> str:
    """
    Find a path to join two tables.
    
    Args:
        kg: Knowledge graph object
        from_table: Source table name
        to_table: Target table name
        
    Returns:
        Formatted string with join path information
    """
    if not kg.is_initialized:
        return "Knowledge graph is not initialized."
        
    # Find path between the tables
    join_path = kg.find_join_path(from_table, to_table)
    if join_path:
        result = f"### Join path between '{from_table}' and '{to_table}':\n"
        for join in join_path:
            result += f"- Join {join['from_table']}.{join['from_column']} with {join['to_table']}.{join['to_column']}\n"
        return result
    else:
        return f"No join path found between '{from_table}' and '{to_table}'."

# Function for suggesting SQL queries for joining tables
async def suggest_sql_query(kg, tables: List[str]) -> str:
    """
    Suggest an SQL query for joining multiple tables.
    
    Args:
        kg: Knowledge graph object
        tables: List of table names to join
        
    Returns:
        Formatted string with SQL query suggestion
    """
    if not kg.is_initialized:
        return "Knowledge graph is not initialized."
        
    # Suggest SQL query for joining tables
    sql_query = kg.suggest_sql_query(tables)
    if sql_query:
        return f"Suggested SQL query for joining {', '.join(tables)}:\n```sql\n{sql_query}\n```"
    else:
        return f"Could not generate SQL suggestion for joining {', '.join(tables)}."

# Main function for interacting with the knowledge graph
async def use_knowledge_graph(kg, action: str, tables: Optional[List[str]] = None, column: Optional[str] = None) -> str:
    """
    Use the knowledge graph to get information about database structure and relationships.
    
    Args:
        kg: Knowledge graph object
        action: The action to perform - one of: "info", "path", "suggest", "samples"
        tables: List of table names to analyze (required for "path" and "suggest" actions)
        column: Column name for "samples" action
        
    Returns:
        Information about table relationships, sample data, join paths, or SQL suggestions
    """
    print('knowledge_graph tool invoked')
    
    if not kg or not hasattr(kg, 'is_initialized'):
        return "Knowledge graph is not available or not properly initialized."
        
    if action == "info" and tables and len(tables) == 1:
        return await get_table_info(kg, tables[0])
            
    elif action == "samples" and tables and len(tables) == 1 and column:
        return await get_column_samples(kg, tables[0], column)
            
    elif action == "path" and tables and len(tables) == 2:
        return await find_join_path(kg, tables[0], tables[1])
            
    elif action == "suggest" and tables and len(tables) >= 2:
        return await suggest_sql_query(kg, tables)
            
    else:
        return "Invalid action or missing parameters. Valid actions are: 'info', 'path', 'suggest', 'samples'."

async def enhanced_knowledge_graph_tool(kg, action: str, tables: Optional[List[str]] = None, column: Optional[str] = None) -> str:
    """
    Enhanced version of the knowledge graph tool with additional contextual information based on action type.
    
    Args:
        kg: Knowledge graph object
        action: The action to perform - one of: "info", "path", "suggest", "samples"
        tables: List of table names to analyze (required for "path" and "suggest" actions)
        column: Column name for "samples" action
        
    Returns:
        Information about table relationships, sample data, join paths, or SQL suggestions with additional context
    """
    # Get the basic result
    result = await use_knowledge_graph(kg, action, tables, column)
    
    # For "samples" action, add a reminder to use the exact values
    if action == "samples" and result and "Sample values" in result:
        result += "\n\nIMPORTANT: Use THESE EXACT VALUES in your SQL WHERE clauses. Do not assume or guess values."
    
    # For "info" action, add a general reminder to check for relationships
    if action == "info" and result and "No relationships" not in result:
        result += "\n\nNote: Check if this table has relationships with other tables that might be relevant to the query."
    
    # For "path" action, emphasize the importance of proper join conditions
    if action == "path" and result and "Join path between" in result:
        result += "\n\nMake sure to use these exact join conditions in your SQL query to correctly relate the data."
    
    return result