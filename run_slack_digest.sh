#!/bin/bash

# Change to the experimental application directory
cd "$(dirname "$0")"

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Run the Flask application on a different port to avoid conflicts
python3 -m flask run --debug --port=5001

# Keep the terminal window open if there's an error
read -p "Press [Enter] to close..." 