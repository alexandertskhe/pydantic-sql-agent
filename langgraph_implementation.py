import os
from typing import List, Dict, Any, Optional, Union, Tuple

# LangGraph imports
from langgraph.graph import END
from langgraph.graph.message import MessageGraph
from langchain_core.messages import HumanMessage, AIMessage

# Import existing agent and tools
from pydantic_agent import create_sql_agent, Dependencies
from knowledge_graph import DBKnowledgeGraph
from utils.db_manager import DatabaseManager
from utils.message_converter import convert_langgraph_to_pydantic_messages

# For typing
from pydantic_ai.usage import UsageLimits

# Node functions for the graph
from typing import List, Any
from pydantic_ai.messages import ModelRequest, ModelResponse, UserPromptPart, TextPart


async def agent_node(messages: List[Any]) -> List[Any]:
    """Process messages through the SQL agent."""
    print("Executing agent node")
    
    # Get the latest user message
    user_message = messages[-1] if messages else None
    if not user_message or not hasattr(user_message, "content"):
        return [AIMessage(content="No valid user message found")]
    
    # Initialize database connection
    db_manager = DatabaseManager('sqlite_db/sqlite.db')
    db = await db_manager.connect()
    
    # Initialize knowledge graph
    kg = DBKnowledgeGraph('sqlite_db/sqlite.db')
    await kg.initialize()
    
    try:
        # Create dependencies for the agent
        deps = Dependencies(db=db, kg=kg)
        
        # Create the SQL agent
        agent_sql = create_sql_agent()
        
        # Convert LangGraph messages to pydantic-ai format for the agent
        agent_message_history = convert_langgraph_to_pydantic_messages(messages)
        
        # Run the agent with correctly formatted message history
        response = await agent_sql.run(
            deps=deps,
            user_prompt=user_message.content,
            usage_limits=UsageLimits(request_limit=10),
            message_history=agent_message_history,
        )
        
        # Return the agent's response as an AIMessage
        return [AIMessage(content=response.data)]
        
    except Exception as e:
        print(f"Error executing agent: {e}")
        import traceback
        traceback.print_exc()
        return [AIMessage(content=f"An error occurred: {str(e)}")]
        
    finally:
        # Clean up resources
        await db_manager.close()

# Create the graph
def create_message_graph():
    """Create and return a configured MessageGraph."""
    # Initialize the graph
    workflow = MessageGraph()
    
    # Add the agent node
    workflow.add_node("agent", agent_node)
    
    # Set entry point (the start node)
    workflow.set_entry_point("agent")
    
    # Set finish point (where execution stops)
    workflow.set_finish_point("agent")
    
    # Compile the graph
    return workflow.compile()

# Function to run the graph
async def run_message_graph(
    user_input: str,
    previous_messages: Optional[List[Any]] = None
) -> Tuple[str, List[Any]]:
    """
    Run the MessageGraph with the given user input and optional message history.
    
    Args:
        user_input: The user query
        previous_messages: Optional list of previous messages for context
        
    Returns:
        Tuple of (agent_response, updated_messages)
    """
    # Create the graph
    graph = create_message_graph()
    
    # Initialize messages with previous messages or empty list
    messages = previous_messages or []
    
    # Add the current user message
    messages.append(HumanMessage(content=user_input))
    
    # Run the graph with the messages
    updated_messages = await graph.ainvoke(messages)
    
    # Get the latest AI message as the response
    for msg in reversed(updated_messages):
        if isinstance(msg, AIMessage):
            response = msg.content
            break
    else:
        response = "No response generated"
    
    # Return both the response text and the updated messages
    return response, updated_messages

# Simple streaming version
async def run_message_graph_stream(
    user_input: str,
    previous_messages: Optional[List[Any]] = None
) -> Tuple[str, List[Any]]:
    """
    Run the MessageGraph with streaming output capability.
    
    Args:
        user_input: The user query
        previous_messages: Optional list of previous messages for context
        
    Returns:
        Tuple of (agent_response, updated_messages)
    """
    # For now, just run the regular graph
    return await run_message_graph(user_input, previous_messages)