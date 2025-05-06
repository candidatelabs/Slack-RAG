import anthropic
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path=".env")

print("ANTHROPIC_API_KEY:", os.getenv("ANTHROPIC_API_KEY"))

client = anthropic.Anthropic()

try:
    message = client.messages.create(
        model="claude-3-7-sonnet-20250219",
        max_tokens=100,
        temperature=1,
        system="You are a world-class poet. Respond only with short poems.",
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "Why is the ocean salty?"
                    }
                ]
            }
        ]
    )
    print("Claude response:", message.content)
except Exception as e:
    print("Error calling Claude API:", e) 