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
from tools.sqlite_db_tool import list_tables_names, describe_table
from tools.sql_query_tool import run_sql_query_enhanced
from tools.knowledge_graph_tool import enhanced_knowledge_graph_tool
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
        The database contains related tables with relevant information. Use the list_tables_tool to discover all available tables.

        MANDATORY WORKFLOW:
        1. ALWAYS start by using list_tables_tool to get a list of all tables in the database.
        2. ALWAYS use describe_table_tool to get a description of each table in the database you'll need.
        3. For questions that may involve relationships between tables, use knowledge_graph_tool with "path" action to check how tables can be joined.
        4. ALWAYS use knowledge_graph_tool with "info" action to explore relevant tables.
        5. CRITICAL: Before writing ANY SQL with WHERE clauses, you MUST use knowledge_graph_tool with "samples" action 
        for EACH column that will be in your WHERE clause.
        Example: knowledge_graph_tool(action="samples", tables=["table_name"], column="column_name")
        6. Only AFTER checking sample data, construct your SQL query using EXACT values from the sample data.
        7. Use the run_sql_query_tool to execute your final SQL query.
        
        QUERY CONSTRUCTION RULES:
        - NEVER assume values for WHERE clauses. ALWAYS check sample data first.
        - Pay attention to case sensitivity - match the exact column names and values.
        - For text fields, consider using LIKE '%value%' instead of exact matches.
        - When searching by categories or status fields, check the EXACT valid values that exist in the database.
        - DO NOT make any DML statements (INSERT, UPDATE, DELETE, DROP etc.) to the database.
        - For complex joins, get relationship information about primary and foreign keys.
        - When joining tables with one-to-many relationships, always use DISTINCT to avoid duplicate rows.
        - When user mentions "download", "export", "extract", or "CSV", use the export_to_csv_tool.
        
        MULTI-TABLE QUERIES:
        - When a question might involve data from multiple areas, always consider if it requires joining tables
        - Use knowledge_graph_tool with "path" action to find join paths between potentially related tables
        - Questions that ask for information from different domains usually require joins
        - Be aware of the relationships between tables and use appropriate JOIN conditions
        - If a query returns no results, consider if you need to join with related tables
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
        4. Check if the query should involve JOINs with related tables
        
        Args:
            sql_query: SQL query to run
            limit: Maximum number of rows to return (default: 10)
        
        Returns:
            Query results in JSON format
        """
        print('run_sql_query tool invoked')
        
        # Use the enhanced SQL query function from the imported module
        return await run_sql_query_enhanced(ctx.deps.db, sql_query, ctx.deps.kg, limit)

    @agent_sql.tool(retries=3)
    async def export_to_csv_tool(ctx: RunContext, sql_query: str, limit: Optional[int] = None) -> str:
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
        print('export_to_csv tool invoked')
        from tools.export_tool import enhanced_export_to_csv
        
        return await enhanced_export_to_csv(ctx.deps.db, sql_query, ctx.deps.kg, limit)

    @agent_sql.tool(retries=10)
    async def knowledge_graph_tool(ctx: RunContext, action: str, tables: Optional[List[str]] = None, column: Optional[str] = None) -> str:
        """
        Use this tool to get information about database tables, their relationships, schemas, and sample data.
        
        IMPORTANT: You MUST use this tool to check sample values for any column you'll use in 
        WHERE clauses BEFORE writing your SQL query to ensure you use actual values from the database.
        
        When a user query might involve multiple tables, use this tool with action="path" to find join paths.
        
        Args:
            action: The action to perform - one of: "info", "path", "suggest", "samples"
            tables: List of table names to analyze (required for "path" and "suggest" actions)
            column: Column name for "samples" action
                
        Returns:
            Information about table relationships, sample data, join paths, or SQL suggestions
        """ 
        return await enhanced_knowledge_graph_tool(ctx.deps.kg, action, tables, column)

    return agent_sql








# # Function to run the agent independently (for testing)
# async def run_agent_test(user_prompt: str, db_path: str = 'sqlite_db/sqlite.db'):
#     from tools.sqlite_db_tool import DatabaseManager
    
#     db_manager = DatabaseManager(db_path)
#     db = await db_manager.connect()
    
#     try:
#         agent_sql = create_sql_agent()
#         deps = Dependencies(db=db)
        
#         async with agent_sql.run_stream(
#             deps=deps, 
#             user_prompt=user_prompt,
#             usage_limits=UsageLimits(request_limit=10)
#         ) as response:
#             async for r in response.stream_text():
#                 print(r)

#     except UsageLimitExceeded as e:
#         print(f'Usage limit exceeded {e}')
#     finally:
#         await db_manager.close()


# # For standalone testing
# if __name__ == "__main__":
#     asyncio.run(run_agent_test('List all tables in the database'))