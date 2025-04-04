import chainlit as cl
from pydantic_ai.exceptions import UsageLimitExceeded
from pydantic_ai.usage import UsageLimits
from collections import deque
import os

from pydantic_agent import create_sql_agent, Dependencies
from utils.db_manager import DatabaseManager
from knowledge_graph.knowledge_graph import DBKnowledgeGraph

from dotenv import load_dotenv
load_dotenv(override=True)


########## Agent ##########
agent_sql = create_sql_agent()

# Initialize knowledge graph
kg = None

MAX_HISTORY_LENGTH = 5
message_histories = {}

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
    
    # Add a loading message for the user
    # await cl.Message(content="Database knowledge graph loaded. I'm ready to help with your SQL queries!").send()

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
        # cl.Starter(
        #     label="Provide me with the number of roadmaps by their status",
        #     message="Provide me with the number of roadmaps by their status",
        #     icon = "/public/icon_1.png",
        #     ),
    ]

@cl.on_message
async def main(message: cl.Message):
    global kg
    
    user_id = message.author

    if user_id not in message_histories:
        message_histories[user_id] = deque(maxlen=MAX_HISTORY_LENGTH)

    db_manager = DatabaseManager('sqlite_db/sqlite.db')
    db = await db_manager.connect()

    msg = cl.Message(content="")
    await msg.send()  # Add this line to send the empty message first

    try:
        deps = Dependencies(db=db, kg=kg)  # Include knowledge graph in dependencies

        user_history = message_histories[user_id]

        async with agent_sql.run_stream(
            deps=deps,
            user_prompt=message.content,
            usage_limits=UsageLimits(request_limit=10),
            message_history=list(user_history) if user_history else None,
        ) as response:
            async for r in response.stream_text(delta=True, debounce_by=0.5):
                await msg.stream_token(r)
                await msg.update()  # Update after each token for better responsiveness
        
        message_histories[user_id].extend(response.new_messages())

        print('-------------------------')
        print(message_histories[user_id])
        print('-------------------------')

    except UsageLimitExceeded as e:
        await msg.update()  # Ensure this is awaited
        print(f'Usage limit exceeded {e}')
    
    finally:
        await db_manager.close()