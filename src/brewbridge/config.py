import os
import dotenv
from langchain_openai import ChatOpenAI

dotenv.load_dotenv()

ASIMOV_URL = os.getenv("ASIMOV_URL")
ASIMOV_PRODUCT_TOKEN = os.getenv("ASIMOV_PRODUCT_TOKEN")

llm_asimov = ChatOpenAI(
    model="openai/gpt-40-mini",
    base_url=f"{ASIMOV_URL}/api/v2",
    api_key=ASIMOV_URL,
    temperature=0.0,
    max_retries=3,
    extra_body={"max_tokens": 1000},  # Ajustar si se llegan a generar salidas muy largas
)
