import asyncio
from typing import Optional, List, Any
import aiosqlite
from dataclasses import dataclass
import re
from pydantic import BaseModel, Field
from pydantic_ai import Agent, RunContext
from pydantic_ai.exceptions import UsageLimitExceeded
from pydantic_ai.usage import UsageLimits

# Import local tools and utils
from tools.sqlite_async import list_tables_names, describe_table, run_sql_query
from tools.export_tool import query_to_csv_file
from tools.knowledge_graph import use_knowledge_graph
from tools.llm_query_enhancer import llm_enhance_query_for_export
from utils.markdown import to_markdown

# Import model
from llm_model import model_openai
# Set model
model = model_openai

########## Pydantic Models ##########
@dataclass
class Dependencies:
    db: aiosqlite.Connection
    kg: Optional[Any] = None

class ResponseModel(BaseModel):
    sql_query_result: str = Field(name='sql_query_result', description="The results of the SQL query.")

########## Create Agent ##########
def create_sql_agent():
    """
    Create and return a configured SQL agent that can be used across the application.
    """
    agent_sql = Agent(
        model=model,
        # result_type=ResponseModel,
        retries=10
    )

    ########## Agent prompt ##########
    @agent_sql.system_prompt
    async def system_prompt(ctx: RunContext) -> str:
        return f"""
        ROLE:
        You are AI Agent, designed to interact with a SQLite database. 
        You have access to the following tools to interact with the database, to get information from knowledge graph and to export data to CSV.

        GOAL:
        Given an input question, create a syntactically correct SQLite query to run. RETURN humanlike answer based on query result.

        CONTEXT:
        Here is a list of tables in the database:
        sis_airports - Table with information about airports, including airport names, IATA codes, and other relevant data.
        sis_wan - List of WAN devices available and their characteristics, including device names, device types, and other relevant data.

        MANDATORY WORKFLOW:
        1. ALWAYS start by using list_tables_tool to get a list of all tables in the database.
        2. ALWAYS use describe_table_tool to get a description of each table in the database you'll need.
        3. ALWAYS use knowledge_graph_tool with "info" action to explore relevant tables.
        4. CRITICAL: Before writing ANY SQL with WHERE clauses, you MUST use knowledge_graph_tool with "samples" action 
        for EACH column that will be in your WHERE clause.
        Example: knowledge_graph_tool(action="samples", tables=["sis_airports"], column="STATUS")
        5. Only AFTER checking sample data, construct your SQL query using EXACT values from the sample data.
        6. Use the run_sql_query_tool to execute your final SQL query.
        
        QUERY CONSTRUCTION RULES:
        - NEVER assume values for WHERE clauses. ALWAYS check sample data first.
        - Pay attention to case sensitivity - match the exact column names and values.
        - For text fields, consider using LIKE '%value%' instead of exact matches.
        - When searching by regions or status, check the EXACT valid values that exist in the database.
        - Example: If user asks for "active airports in EUR region", you MUST:
        * First check sample values for STATUS column
        * First check sample values for AIRPORT_REGION column
        * Use those EXACT values in your WHERE clause (not just 'Active' or 'EUR' if those aren't the exact values)
        - DO NOT make any DML statements (INSERT, UPDATE, DELETE, DROP etc.) to the database.
        - For complex joins, get relationship information about primary and foreign keys.
        - When joining tables with one-to-many relationships (like sis_airports to sis_wan), always use DISTINCT.
        - When user mentions "download", "export", "extract", or "CSV", use the export_to_csv_tool.
        - When searching for airports use AIRPORT_IATA instead of AIRPORT_NAME. ALSO provide AIRPORT_HUB_ID.
        """

    ########## Tools ##########
    @agent_sql.tool(retries=10)
    async def list_tables_tool(ctx: RunContext) -> str:
        print('list_tables tool invoked')
        """Use this tool to get a list of all tables in the database."""
        database_tables = await list_tables_names(ctx.deps.db)
        return f"Database tables: {to_markdown(database_tables)}"

    @agent_sql.tool(retries=10)
    async def describe_table_tool(ctx: RunContext, table_name: str) -> str:
        print('describe_table tool invoked')
        """Use this tool to get a description of a table in the database."""
        return await describe_table(ctx.deps.db, table_name)

    @agent_sql.tool(retries=10)
    async def run_sql_query_tool(ctx: RunContext, sql_query: str, limit: Optional[int] = 10) -> str:
        """
        Use this tool to run a SQL query on the database.
        
        IMPORTANT: If your query returns no results:
        1. Double check your WHERE clause values against sample data
        2. Consider using LIKE operators for text fields
        3. Try case-insensitive comparison for text fields
        
        Args:
            sql_query: SQL query to run
            limit: Maximum number of rows to return (default: 10)
        
        Returns:
            Query results in JSON format
        """
        print('run_sql_query tool invoked')
        
        # Execute the original query
        result = await run_sql_query(ctx.deps.db, sql_query, limit)
        
        # Check if the result is empty (no rows returned)
        if result == '[]':
            print("Original query returned no results, attempting fallback strategies")
            
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
                        cursor = await ctx.deps.db.execute(f"PRAGMA table_info({table_name});")
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
                            modified_result = await run_sql_query(ctx.deps.db, modified_query, limit)
                            
                            if modified_result != '[]':
                                return modified_result + "\n\nNote: Results found using partial matching (LIKE operator) instead of exact matches."
                    except Exception as e:
                        print(f"Error in fallback query strategy: {e}")
        
        return result


    @agent_sql.tool(retries=3)
    async def export_to_csv_tool(ctx: RunContext, sql_query: str, limit: Optional[int] = None) -> str:
        print('export_to_csv tool invoked')
        """Use this tool to generate a CSV export from a SQL query.
        This tool will run the query, save results as a CSV file, and provide a download URL.
        
        IMPORTANT: When exporting data, ensure your query includes more comprehensive columns 
        than just the minimum. Include identifying fields, status fields, and other relevant
        data to make the export more useful.
        
        Args:
            sql_query: The SQL query to run and export results from. Should include comprehensive columns.
            limit: Optional limit on number of rows (defaults to all rows)
        
        Returns:
            Information about the generated CSV file including the download URL
        """

        # Check if the query needs enhancement for export
        try:
            # Pass the knowledge graph to the enhancer function
            enhanced_query = await llm_enhance_query_for_export(ctx.deps.db, sql_query, kg=ctx.deps.kg)
            if enhanced_query != sql_query:
                print(f"Original query: {sql_query}")
                print(f"Enhanced query: {enhanced_query}")
                sql_query = enhanced_query
        except Exception as e:
            print(f"Error enhancing query: {e}")
            # Continue with original query if enhancement fails
        
        # Generate the CSV file and get the download URL
        filename, download_url, row_count = await query_to_csv_file(ctx.deps.db, sql_query, limit)
        
        # Debug log the results
        print(f"CSV Export result: filename={filename}, url={download_url}, rows={row_count}")
        
        if not filename or not download_url:
            return "Failed to generate CSV file. Please check the query syntax and try again."
        
        if row_count == 0:
            return "The query returned no results. Please modify your query to return data."
        
        return f"""
    CSV export generated successfully with {row_count} rows of data.

    You can download the file here: [Download {filename}]({download_url})
    """

    @agent_sql.tool(retries=10)
    async def knowledge_graph_tool(ctx: RunContext, action: str, tables: Optional[List[str]] = None, column: Optional[str] = None) -> str:
        """
        Use this tool to get information about database tables, their relationships, schemas, and sample data.
        
        IMPORTANT: You MUST use this tool to check sample values for any column you'll use in 
        WHERE clauses BEFORE writing your SQL query to ensure you use actual values from the database.
        
        Args:
            action: The action to perform - one of: "info", "path", "suggest", "samples"
            tables: List of table names to analyze (required for "path" and "suggest" actions)
            column: Column name for "samples" action
                
        Returns:
            Information about table relationships, sample data, join paths, or SQL suggestions
        """
        result = await use_knowledge_graph(ctx.deps.kg, action, tables, column)
        
        # For "samples" action, add a reminder to use the exact values
        if action == "samples" and result and "Sample values" in result:
            result += "\n\nIMPORTANT: Use THESE EXACT VALUES in your SQL WHERE clauses. Do not assume or guess values."
            
        return result

    return agent_sql

# Function to run the agent independently (for testing)
async def run_agent_test(user_prompt: str, db_path: str = 'sqlite_db/sqlite.db'):
    from tools.sqlite_async import DatabaseManager
    
    db_manager = DatabaseManager(db_path)
    db = await db_manager.connect()
    
    try:
        agent_sql = create_sql_agent()
        deps = Dependencies(db=db)
        
        async with agent_sql.run_stream(
            deps=deps, 
            user_prompt=user_prompt,
            usage_limits=UsageLimits(request_limit=10)
        ) as response:
            async for r in response.stream_text():
                print(r)

    except UsageLimitExceeded as e:
        print(f'Usage limit exceeded {e}')
    finally:
        await db_manager.close()


# For standalone testing
if __name__ == "__main__":
    asyncio.run(run_agent_test('List all tables in the database'))