import chainlit as cl
from typing import Dict, List, Any
import os

# Import the graph implementation
from langgraph_implementation import run_graph
from knowledge_graph.knowledge_graph import DBKnowledgeGraph

from dotenv import load_dotenv
load_dotenv(override=True)

# Initialize knowledge graph (just for initialization, not used directly in app)
kg = None

# Store message history per user
user_messages = {}

########## Chainlit ##########
@cl.on_chat_start
async def on_chat_start():
    global kg
    
    # Initialize knowledge graph on startup
    db_path = 'sqlite_db/sqlite.db'
    
    # Initialize the knowledge graph
    kg = DBKnowledgeGraph(db_path)
    await kg.initialize()
    
    print(f"Knowledge graph initialized from {db_path}")

@cl.set_starters
async def set_starters():
    return [
        cl.Starter(
            label="List tables available in the database",
            message="List tables available in the database",
            icon = "/public/icon_1.png",
            ),
        cl.Starter(
            label="Provide schema of the tables in the database",
            message="Provide schema of the tables in the database",
            icon = "/public/icon_1.png",
            ),
        cl.Starter(
            label="Show me airports with their WAN routers",
            message="Show me top 10 airports with their WAN devices and their characteristics",
            icon = "/public/icon_1.png",
            ),
        cl.Starter(
            label="Show me airports in EUR region with active status",
            message="Show me airports in EUR region with active status",
            icon = "/public/icon_1.png",
            ),
    ]

@cl.on_message
async def main(message: cl.Message):
    user_id = message.author

    # Create the empty message that we'll stream to
    msg = cl.Message(content="")
    await msg.send()

    try:
        # Get existing messages for this user if available
        previous_messages = user_messages.get(user_id, [])
        
        # Use the graph
        response, updated_messages = await run_graph(
            user_input=message.content,
            previous_messages=previous_messages
        )
        
        # Store the updated messages for next interaction
        user_messages[user_id] = updated_messages
        
        # Stream the response to the UI
        for char in response:
            await msg.stream_token(char)
        await msg.update()
        
        # Debug: Print conversation history
        print('-------------------------')
        print(f"Messages for user {user_id}")
        for i, msg_obj in enumerate(updated_messages):
            # Extract role and content
            role = msg_obj.type
            content = msg_obj.content
            print(f"{i}: {role} - {content[:100]}...")
        print('-------------------------')

    except Exception as e:
        # Update the message properly
        await msg.stream_token(f"An error occurred: {str(e)}")
        await msg.update()
        print(f'Error during execution: {e}')
        import traceback
        traceback.print_exc()

# Clear user messages when session ends
@cl.on_chat_end
async def on_chat_end():
    user_id = cl.user_session.get("user_id")
    if user_id in user_messages:
        del user_messages[user_id]
        print(f"Cleared messages for user {user_id}")