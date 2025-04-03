import asyncio
from typing import Optional, List, Any
import aiosqlite
from dataclasses import dataclass
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

        TOOL SELECTION STRATEGY:
        - FIRST, ALWAYS use list_tables_tool to get a list of all tables in the database.
        - SECOND, ALWAYS use describe_table_tool to get a description of a table in the database.
        - THIRD, before constructing a SQL query, ALWAYS use knowledge_graph_tool to get sample data, use this sample data to improve SQL qeuery.
        - You can use knowledge_graph_tool to further improve your SQL queries:
        * Getting detailed information about tables
        * Understanding table relationships and join paths
        * Viewing sample data to understand table contents
        * Getting representative column values
        * Generating SQL suggestions for complex queries
        
        INSTRUCTIONS:
        1. For most queries, use the knowledge_graph_tool with "info" action to explore relevant tables.
        2. For complex queries requiring joins, use knowledge_graph_tool with "path" action to determine optimal join paths.
        3. When you need sample column values, use knowledge_graph_tool with "samples" action.
        4. For complex multi-table queries, use knowledge_graph_tool with "suggest" action to get recommended SQL.
        5. Always use the run_sql_query_tool to execute your final SQL query.
        6. If the user asks for data export, CSV, or downloadable results, use the export_to_csv_tool and provide the download URL.
        7. When searching for airports use AIRPORT IATA instead of AIRPORT_NAME. ALSO provice AIRPORT_HUB_ID. Do not provide AIPORT_NAME in answer, only IATA and HUB_ID.

        QUERY CONSTRUCTION BEST PRACTICES:
        - Pay attention to case sensitivity when constructing queries - match the exact column names.
        - When applying WHERE statements try to use LIKE statements instead of exact matches.
        - DO NOT make any DML statements (INSERT, UPDATE, DELETE, DROP etc.) to the database.
        - For complex joins, get relationship information about primary and foreign keys from the knowledge graph.
        - When joining tables with one-to-many relationships (like sis_airports to sis_wan), always use DISTINCT to avoid duplicate entries. An airport may have multiple WAN devices, so queries joining these tables should use DISTINCT for airport data.
        - For aggregation queries (COUNT, SUM, etc.), make sure to use appropriate GROUP BY clauses.
        - When user mentions "download", "export", "extract", or "CSV", always generate a CSV file.
        - Present download links clearly so users can access their data.
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
        print('run_sql_query tool invoked')
        """Use this tool to run a SQL query on the database. Double check your query before executing it.
        If query returns no results, check sample data from knowledge graph and try again. 
        If an error is returned, rewrite the query, check the query, and try again."""
        return await run_sql_query(ctx.deps.db, sql_query, limit)


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
        Use this tool to get information about database  tables, their relationships, schemas, and sample data.
        
        Args:
            action: The action to perform - one of: "info", "path", "suggest", "samples"
            tables: List of table names to analyze (required for "path" and "suggest" actions)
            column: Column name for "samples" action
            
        Returns:
            Information about table relationships, sample data, join paths, or SQL suggestions
        """
        return await use_knowledge_graph(ctx.deps.kg, action, tables, column)

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