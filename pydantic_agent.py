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
from tools.csv_export import query_to_csv_file
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
        You are AI Agent, designed to interact with a SQLite database. You have access to both direct database tools and an advanced knowledge graph for query optimization.

        GOAL:
        Given an input question, create a syntactically correct SQLite query to run. RETURN humanlike answer based on query result.
        If user requests data export or CSV, use the export_to_csv_tool after getting query results.

        CONTEXT:
        You have access to following tables:
        project_plans - Project plans table, it lists tasks related to the particular project.
        project_roadmaps - Project roadmaps table, it lists projects plan roadmaps.
        sis_airports - Table with information about airports, including airport names, IATA codes etc.
        sis_wan - List of devices available, it's linked with sis_airports via primary and foreign keys.

        TOOL SELECTION STRATEGY:
        - Use the knowledge_graph_tool for:
        * Understanding table relationships and join paths
        * Viewing sample data to understand table contents
        * Getting representative column values
        * Generating SQL suggestions for complex queries
        
        - Use the list_tables_tool and describe_table_tool:
        * When you need to verify table existence or schema
        * When knowledge graph information is insufficient
        
        INSTRUCTIONS:
        1. For most queries, use the knowledge_graph_tool first with "info" action to explore relevant tables.
        2. For complex queries requiring joins, use knowledge_graph_tool with "path" action to determine optimal join paths.
        3. When you need sample column values, use knowledge_graph_tool with "samples" action.
        4. For complex multi-table queries, use knowledge_graph_tool with "suggest" action to get recommended SQL.
        5. Only fall back to list_tables_tool and describe_table_tool when knowledge graph doesn't provide sufficient information.
        6. Always use the run_sql_query_tool to execute your final SQL query.
        7. If the user asks for data export, CSV, or downloadable results, use the export_to_csv_tool and provide the download URL.

        QUERY CONSTRUCTION BEST PRACTICES:
        - Pay attention to case sensitivity when constructing queries - match the exact column names.
        - DO NOT make any DML statements (INSERT, UPDATE, DELETE, DROP etc.) to the database.
        - For complex joins, get relationship information about primary and foreign keys from the knowledge graph.
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
    async def run_sql_query_tool(ctx: RunContext, sql_query: str, limit: Optional[int] = 5) -> str:
        print('run_sql_query tool invoked')
        """Use this tool to run a SQL query on the database. Double check your query before executing it. 
        If an error is returned, rewrite the query, check the query, and try again."""
        return await run_sql_query(ctx.deps.db, sql_query, limit)

    @agent_sql.tool(retries=3)
    async def export_to_csv_tool(ctx: RunContext, sql_query: str, limit: Optional[int] = None) -> str:
        print('export_to_csv tool invoked')
        """Use this tool to generate a CSV export from a SQL query.
        This tool will run the query, save results as a CSV file, and provide a download URL.
        
        Args:
            sql_query: The SQL query to run and export results from
            limit: Optional limit on number of rows (defaults to all rows)
        
        Returns:
            Information about the generated CSV file including the download URL
        """
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
        print('knowledge_graph tool invoked')
        """
        Use this tool to get information from the knowledge graph about table relationships and sample data.
        
        Args:
            action: The action to perform - one of: "info", "path", "suggest", "samples"
            tables: List of table names to analyze (required for "path" and "suggest" actions)
            column: Column name for "samples" action
            
        Returns:
            Information about table relationships, sample data, join paths, or SQL suggestions
        """
        kg = ctx.deps.kg
        
        if not kg.is_initialized:
            return "Knowledge graph is not initialized."
            
        if action == "info" and tables and len(tables) == 1:
            # Get info about a single table
            table_info = kg.get_table_info(tables[0])
            
            # Format the response to be more readable
            result = f"### Table: {tables[0]}\n"
            
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
            
        elif action == "samples" and tables and len(tables) == 1 and column:
            # Get sample values for a specific column
            column_info = kg.get_column_values(tables[0], column)
            
            if "error" in column_info:
                return column_info["error"]
                
            result = f"### Sample values for {tables[0]}.{column}:\n"
            
            # Add sample values
            if "sample_values" in column_info and column_info["sample_values"]:
                result += "Sample values: " + ", ".join([str(v) for v in column_info["sample_values"]]) + "\n\n"
            
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
                    result += "- Most common values:\n"
                    for val in stats["common_values"]:
                        result += f"  - {val['value']} (appears {val['count']} times)\n"
            
            return result
            
        elif action == "path" and tables and len(tables) == 2:
            # Find path between two tables
            join_path = kg.find_join_path(tables[0], tables[1])
            if join_path:
                result = f"### Join path between '{tables[0]}' and '{tables[1]}':\n"
                for join in join_path:
                    result += f"- Join {join['from_table']}.{join['from_column']} with {join['to_table']}.{join['to_column']}\n"
                return result
            else:
                return f"No join path found between '{tables[0]}' and '{tables[1]}'."
                    
        elif action == "suggest" and tables and len(tables) >= 2:
            # Suggest SQL query for joining tables
            sql_query = kg.suggest_sql_query(tables)
            if sql_query:
                return f"Suggested SQL query for joining {', '.join(tables)}:\n```sql\n{sql_query}\n```"
            else:
                return f"Could not generate SQL suggestion for joining {', '.join(tables)}."
                    
        else:
            return "Invalid action or missing parameters. Valid actions are: 'info', 'path', 'suggest', 'samples'."

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