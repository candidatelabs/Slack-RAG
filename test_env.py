import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
 
# Print the environment variables
print("SLACK_USER_TOKEN:", os.getenv("SLACK_USER_TOKEN"))
print("ANTHROPIC_API_KEY:", os.getenv("ANTHROPIC_API_KEY")) 