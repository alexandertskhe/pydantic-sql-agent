import os
from dotenv import load_dotenv

from pydantic_ai.models.openai import OpenAIModel
from pydantic_ai.providers.azure import AzureProvider

# from .deepseek_azure_ai_foundry_model import AzureAIFoundryModel

load_dotenv(override=True)

model_openai = OpenAIModel(
    model_name=os.getenv("azure_deployment_name_gpt_4o_mini"),
    provider=AzureProvider(
        azure_endpoint=os.getenv("azure_endpoint_gpt_4o_mini"),
        api_version=os.getenv("azure_api_version"),
        api_key=os.getenv("azure_openai_api_key_gpt_4o_mini"),
    ),
)


###########################################################
# model_deepseek_v3 = AzureAIFoundryModel(
#     model_name="DeepSeek-V3",
#     endpoint=os.getenv("azure_deepseek_v3_endpoint"),
#     api_key=os.getenv("azure_deepseek_v3_api_key"),
# )