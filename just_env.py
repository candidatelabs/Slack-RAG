import os
from dotenv import load_dotenv
 
print("Current working directory:", os.getcwd())
load_dotenv(dotenv_path=".env")
print("ANTHROPIC_API_KEY:", os.getenv("ANTHROPIC_API_KEY")) 