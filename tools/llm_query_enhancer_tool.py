import json
import re
import os
from typing import List, Dict, Any
from dotenv import load_dotenv
from openai import AzureOpenAI  # Using Azure OpenAI client based on your configuration

# Load environment variables
load_dotenv(override=True)

# Initialize the Azure OpenAI client
client = AzureOpenAI(
    api_key=os.getenv("azure_open_ai_api_key_gpt_4o"),
    api_version=os.getenv("azure_api_version"),
    azure_endpoint=os.getenv("azure_endpoint_gpt_4o"),
)
deployment_name = os.getenv("azure_deployment_name_gpt_4o")

async def llm_enhance_query_for_export(db, sql_query: str, kg=None) -> str:
    """
    Use OpenAI directly to enhance a SQL query for export by analyzing it and adding more columns.
    Uses the knowledge graph if available for getting schema information.
    
    Args:
        db: Database connection
        sql_query: Original SQL query
        kg: Knowledge graph object (optional)
        
    Returns:
        Enhanced SQL query with more columns
    """
    # Skip enhancement for certain types of queries
    if "select *" in sql_query.lower():
        return sql_query
    
    try:
        # Get table schema information either from knowledge graph or directly from DB
        if kg and kg.is_initialized:
            table_info = get_table_info_from_kg(kg, sql_query)
        else:
            table_info = await extract_and_get_table_schemas(db, sql_query)
        
        if not table_info:
            print("Couldn't extract table information, using original query")
            return sql_query
        
        # Prepare the prompt for the LLM
        system_message = "You are a SQL expert that enhances queries to include more columns for data exports while preserving the original query logic."
        
        user_message = f"""
I have a SQL query that I want to enhance for exporting data to CSV. I want to add more relevant columns to make the export more comprehensive while preserving the original query logic.

Original query:
```sql
{sql_query}
```

Here's the schema information for tables in this query:
{table_info}

Please enhance this query by:
1. Add more relevant columns that would be useful in an export (especially ID fields, name fields, status fields, and region fields)
2. Maintain the original WHERE clauses and JOIN conditions
3. Keep the DISTINCT if it exists in the original
4. Preserve the table aliases from the original query
5. Maintain the exact same result set filtering
6. IMPORTANT: Always include all columns used in JOIN conditions and WHERE clauses in the SELECT list

Return only the enhanced SQL query without any explanation or markdown formatting.
"""
        # Use the OpenAI client directly
        response = client.chat.completions.create(
            model=deployment_name,
            messages=[
                {"role": "system", "content": system_message},
                {"role": "user", "content": user_message}
            ],
            temperature=0.1  # Lower temperature for more deterministic SQL generation
        )
        
        # Extract the generated query
        enhanced_query = response.choices[0].message.content.strip()
        
        # Remove any markdown or explanation from the response
        enhanced_query = re.sub(r'^```sql\s*', '', enhanced_query)
        enhanced_query = re.sub(r'\s*```$', '', enhanced_query)
        
        # Basic validation: ensure it's a SELECT query
        if enhanced_query.upper().startswith("SELECT"):
            print(f"LLM enhanced query: {enhanced_query}")
            return enhanced_query
        else:
            print("LLM response does not appear to be a valid SQL query")
            return sql_query
            
    except Exception as e:
        print(f"Error using LLM to enhance query: {e}")
        import traceback
        traceback.print_exc()
        # Fallback to original query if anything goes wrong
        return sql_query

def get_table_info_from_kg(kg, sql_query: str) -> str:
    """
    Extract table names from a SQL query and get their schema information from the knowledge graph.
    
    Args:
        kg: Knowledge graph object
        sql_query: SQL query
        
    Returns:
        Formatted string with table schema information
    """
    tables = extract_tables_from_query(sql_query)
    
    if not tables:
        return ""
    
    # Get schema for each table from knowledge graph
    schema_info = []
    for table_name in tables:
        try:
            # Get table info from knowledge graph
            table_info = kg.get_table_info(table_name)
            
            if not table_info or "error" in table_info:
                print(f"Error getting information for table {table_name} from knowledge graph")
                continue
                
            schema_info.append(f"Table: {table_name}")
            schema_info.append("Columns:")
            
            # Add column information
            columns = table_info.get("columns", [])
            for col in columns:
                col_name = col.get("name", "Unknown")
                col_type = col.get("type", "Unknown")
                is_primary = "PRIMARY KEY" if col_name in table_info.get("primary_keys", []) else ""
                schema_info.append(f"- {col_name} ({col_type}) {is_primary}")
            
            # Add relationship information
            if "relationships" in table_info and table_info["relationships"]:
                schema_info.append("Relationships:")
                for rel in table_info["relationships"]:
                    if "target_table" in rel:
                        schema_info.append(f"- References {rel['target_table']} via {rel['from_column']} -> {rel['to_column']}")
                    elif "source_table" in rel:
                        schema_info.append(f"- Referenced by {rel['source_table']} via {rel['from_column']} -> {rel['to_column']}")
            
            schema_info.append("")
        except Exception as e:
            print(f"Error retrieving table info for {table_name} from knowledge graph: {e}")
    
    return "\n".join(schema_info)

def extract_tables_from_query(sql_query: str) -> List[str]:
    """Extract table names from a SQL query using regex."""
    sql_lower = sql_query.lower()
    tables = []
    
    # Extract tables from FROM clause
    from_match = re.search(r'from\s+([a-zA-Z0-9_]+)(\s+as\s+[a-zA-Z0-9_]+|\s+[a-zA-Z0-9_]+)?', sql_lower)
    if from_match:
        table_name = from_match.group(1)
        tables.append(table_name)
    
    # Extract tables from JOIN clauses
    join_matches = re.finditer(r'join\s+([a-zA-Z0-9_]+)(\s+as\s+[a-zA-Z0-9_]+|\s+[a-zA-Z0-9_]+)?', sql_lower)
    for match in join_matches:
        table_name = match.group(1)
        tables.append(table_name)
    
    # Deduplicate tables
    return list(set(tables))

async def extract_and_get_table_schemas(db, sql_query: str) -> str:
    """
    Extract table names from a SQL query and fetch their schemas directly from the database.
    Used as a fallback when knowledge graph is not available.
    
    Args:
        db: Database connection
        sql_query: SQL query
        
    Returns:
        Formatted string with table schema information
    """
    tables = extract_tables_from_query(sql_query)
    
    if not tables:
        return ""
    
    # Get schema for each table
    schema_info = []
    for table in tables:
        try:
            cursor = await db.execute(f"PRAGMA table_info({table});")
            columns = await cursor.fetchall()
            
            schema_info.append(f"Table: {table}")
            schema_info.append("Columns:")
            for col in columns:
                col_name = col[1]
                col_type = col[2]
                is_primary = "PRIMARY KEY" if col[5] == 1 else ""
                schema_info.append(f"- {col_name} ({col_type}) {is_primary}")
            schema_info.append("")
        except Exception as e:
            print(f"Error getting schema for {table}: {e}")
    
    return "\n".join(schema_info)