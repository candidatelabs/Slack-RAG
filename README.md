# Slack Data Analyzer

A powerful Slack data analysis tool focused on candidate activity, built with PyQt6, SQLite, and AI-powered semantic search.

Effortlessly syncs Slack conversations from key client channels (candidatelabs, candidate-labs) based on user and date range.

Local Data Warehouse (SQLite): All Slack messages, threads, and sync logs are stored locally for blazing-fast retrieval—no more redundant API calls.

Semantic Search Engine (ChromaDB): Every message is transformed into a semantic vector using OpenAI embeddings, allowing deep, context-aware search across conversations.

RAG-Powered Smart Querying: Using Retrieval-Augmented Generation (RAG) with Claude, users can ask any custom question and get responses backed by real Slack history—not just generic AI guesses.

Custom Prompting to query against slack data



How It Works (High-Level Flow):
User Inputs: Email + date range → Choose "Force Refresh" or use cached data.

Slack Sync: Fetches all messages and threads from relevant channels → Stored in SQLite.

Semantic Indexing: Every message is embedded with OpenAI embeddings → Indexed in ChromaDB.

Smart Querying: User prompts trigger semantic search → Relevant messages are pulled → Claude provides a context-aware answer.

Lightning-Fast Access: If the data is cached, queries are near-instantaneous.



## Features

- Client-centric (channel-centric) analysis
- Candidate extraction from messages
- Semantic search using OpenAI embeddings
- AI-powered summarization using Claude
- Persistent caching with TTL
- Rate limiting and exponential backoff
- Connection pooling
- Structured logging and metrics
- Progress tracking and parallel processing

## Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd slack-digest-experimental
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file with your API keys:
```env
SLACK_TOKEN=xoxb-your-token
OPENAI_API_KEY=sk-your-key
ANTHROPIC_API_KEY=sk-your-key
```

5. (Optional) Create a `config.yaml` file for custom settings:
```yaml
db:
  pool_size: 5
  max_overflow: 10
  pool_timeout: 30
  pool_recycle: 3600

api:
  rate_limit_calls: 50
  rate_limit_period: 60
  max_retries: 3
  retry_delay: 1

cache:
  max_size: 1000
  ttl: 3600

log_level: INFO
max_workers: 5
batch_size: 100
```

## Usage

1. Run the application:
```bash
python main.py
```

2. Select timeframe to fetch slack data for
3. Write a natual language prompt to query the data
4. Generate summaries using Claude

## Architecture

- `config.py`: Configuration management
- `logger.py`: Structured logging and metrics
- `database.py`: Database connection pooling
- `cache.py`: Persistent caching
- `rate_limiter.py`: API rate limiting
- `slack_analyzer_core.py`: Core analysis logic
- `candidate_extractor.py`: Candidate extraction
- `slack_rag_backend.py`: RAG pipeline

## Monitoring

Metrics are available at `http://localhost:8000/metrics`:
- API call counts and latencies
- Cache hit/miss rates
- Database operation counts
- Processing times

## Development

1. Install development dependencies:
```bash
pip install -r requirements-dev.txt
```

2. Run tests:
```bash
pytest
```

3. Run linting:
```bash
flake8
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

MIT License 
