from pydantic_ai.messages import ModelRequest, ModelResponse, UserPromptPart, TextPart
import datetime

def convert_langgraph_to_pydantic_messages(messages):
    """
    Convert LangGraph messages to pydantic-ai message format.
    
    Args:
        messages: List of LangGraph messages
        
    Returns:
        List of pydantic-ai messages (ModelRequest and ModelResponse objects)
    """
    agent_message_history = []
    
    # Start with index 0 and go through all but the last message (which is the current user message)
    for i in range(0, len(messages) - 1, 2):
        if i + 1 < len(messages):  # Make sure we have a pair
            user_msg = messages[i]
            assistant_msg = messages[i + 1]
            
            # Create a ModelRequest for the user message
            request = ModelRequest(
                parts=[
                    UserPromptPart(
                        content=user_msg.content,
                        timestamp=datetime.datetime.now(datetime.timezone.utc),
                        part_kind='user-prompt'
                    )
                ],
                kind='request'
            )
            
            # Create a ModelResponse for the assistant message
            response = ModelResponse(
                parts=[
                    TextPart(
                        content=assistant_msg.content,
                        part_kind='text'
                    )
                ],
                model_name='gpt-4o',  # This is just a placeholder
                timestamp=datetime.datetime.now(datetime.timezone.utc),
                kind='response'
            )
            
            # Add both to the message history
            agent_message_history.append(request)
            agent_message_history.append(response)
    
    print(f"Passing {len(agent_message_history)} pydantic-ai messages to agent")
    
    return agent_message_history