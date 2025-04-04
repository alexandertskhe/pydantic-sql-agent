import os
from typing import List, Dict, Any, Optional, Union, Tuple, TypedDict

# LangGraph imports
from langgraph.graph import END, StateGraph
from langchain_core.messages import HumanMessage, AIMessage

# Import existing agent and tools
from pydantic_agent import create_sql_agent, Dependencies
from knowledge_graph import DBKnowledgeGraph
from utils.db_manager import DatabaseManager
from utils.message_converter import convert_langgraph_to_pydantic_messages

# For typing
from pydantic_ai.usage import UsageLimits

# Define State TypedDict
class State(TypedDict):
    messages: List[Any]
    query_result: Optional[str]

async def agent_node(state: State) -> State:
    """Process messages through the SQL agent."""
    print("Executing agent node")
    
    messages = state.get("messages", [])
    
    # Get the latest user message
    user_message = messages[-1] if messages else None
    if not user_message or not hasattr(user_message, "content"):
        return {"messages": messages + [AIMessage(content="No valid user message found")], "query_result": None}
    
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
        
        # Update state with the agent's response
        new_messages = messages + [AIMessage(content=response.data)]
        return {"messages": new_messages, "query_result": response.data}
        
    except Exception as e:
        print(f"Error executing agent: {e}")
        import traceback
        traceback.print_exc()
        error_message = f"An error occurred: {str(e)}"
        return {"messages": messages + [AIMessage(content=error_message)], "query_result": error_message}
        
    finally:
        # Clean up resources
        await db_manager.close()

# Create the graph
def create_graph():
    """Create and return a configured StateGraph with state management."""
    workflow = StateGraph(State)
    
    # Add the agent node
    workflow.add_node("agent", agent_node)
    
    # Set entry point (the start node)
    workflow.set_entry_point("agent")
    
    # Add an edge from the agent node to END
    workflow.add_edge("agent", END)
    
    # Compile the graph
    return workflow.compile()

# Function to run the graph
async def run_graph(
    user_input: str,
    previous_messages: Optional[List[Any]] = None
) -> Tuple[str, List[Any]]:
    """
    Run the StateGraph with the given user input and optional message history.
    
    Args:
        user_input: The user query
        previous_messages: Optional list of previous messages for context
        
    Returns:
        Tuple of (agent_response, updated_messages)
    """
    # Create the graph
    graph = create_graph()
    
    # Initialize messages with previous messages or empty list
    messages = previous_messages or []
    
    # Add the current user message
    messages.append(HumanMessage(content=user_input))
    
    # Initialize the state
    initial_state = {"messages": messages, "query_result": None}
    
    # Run the graph with the state
    final_state = await graph.ainvoke(initial_state)
    
    # Get the latest AI message as the response
    updated_messages = final_state["messages"]
    query_result = final_state["query_result"]
    
    # Return both the response text and the updated messages
    return query_result or "No response generated", updated_messages
