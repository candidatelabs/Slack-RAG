# Slack Digest Generator

A Mac application that generates comprehensive candidate pipeline reports from Slack channels.

## Features

- Native Mac GUI interface
- Date range selection with calendar popup
- Real-time progress updates
- Automatic saving of generated reports
- Support for multiple timezones
- Candidate pipeline tracking and reporting

## Setup

1. Install Python 3.8 or later if you haven't already
2. Clone this repository
3. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Create a `.env` file in the project root with your API keys:
   ```
   SLACK_USER_TOKEN=xoxp-your-slack-token
   ANTHROPIC_API_KEY=your-anthropic-api-key
```

## Running the Application

1. Run the application:
```bash
   python slack_digest_app.py
   ```

2. In the application:
   - Enter your Slack email
   - Select the date range
   - Choose your timezone
   - Click "Generate Digest"

3. The generated report will be saved as a markdown file in the current directory

## Requirements

- macOS 10.15 or later
- Python 3.8 or later
- Slack User Token with appropriate permissions
- Anthropic API key

## Notes

- The application requires a Slack User Token with permissions to read channel messages
- The Anthropic API key is used for generating the candidate pipeline reports
- Generated reports are saved in markdown format for easy reading and sharing 