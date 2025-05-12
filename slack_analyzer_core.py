import os
import sys
from datetime import datetime, timedelta
import pytz
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import anthropic
import logging
from dotenv import load_dotenv
import sqlite3
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from slack_analyzer import SlackDataStore  # Adjust import if needed
from typing import List, Dict, Optional, Callable
from logger import get_logger, log_metrics
from config import APIConfig
from database import DatabaseManager
from cache import PersistentCache
from rate_limiter import RateLimiter
from candidate_extractor import CandidateExtractor
from slack_rag_backend import SlackRAGBackend

# Load environment variables
load_dotenv()

logger = get_logger(__name__)

class SlackAnalyzerCore:
    def __init__(
        self,
        config: APIConfig,
        db_manager: DatabaseManager,
        cache: PersistentCache,
        rate_limiter: RateLimiter,
        candidate_extractor: CandidateExtractor,
        rag_backend: SlackRAGBackend
    ):
        self.config = config
        self.db_manager = db_manager
        self.cache = cache
        self.rate_limiter = rate_limiter
        self.candidate_extractor = candidate_extractor
        self.rag_backend = rag_backend
        self.client_channel_pattern = re.compile(r'(?:candidate-labs|candidatelabs)[-\s]+([^-\s]+)', re.IGNORECASE)
        self.token = os.getenv("SLACK_TOKEN")
        self.anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
        if not self.token:
            raise RuntimeError("SLACK_TOKEN environment variable not set")
        if not self.anthropic_api_key:
            raise RuntimeError("ANTHROPIC_API_KEY environment variable not set")
        self.client = WebClient(token=self.token)
        self.claude = anthropic.Anthropic(api_key=self.anthropic_api_key)
        self.timezone = pytz.timezone("America/Chicago")
        self.db_path = config.db_path
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.create_tables()
        self.data_store = SlackDataStore(self.db_path)
        self.data_store.conn = self.conn  # Ensure both use the same connection

    def create_tables(self):
        with self.conn:
            self.conn.execute('''CREATE TABLE IF NOT EXISTS channels (id TEXT PRIMARY KEY, name TEXT, is_member BOOLEAN, is_archived BOOLEAN, last_updated INTEGER)''')
            self.conn.execute('''CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT, username TEXT, email TEXT, last_updated INTEGER)''')
            self.conn.execute('''CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, channel_id TEXT, channel_name TEXT, user_id TEXT, timestamp REAL, datetime TEXT, text TEXT, thread_ts TEXT, is_thread_parent BOOLEAN, has_linkedin_url BOOLEAN)''')
            self.conn.execute('''CREATE TABLE IF NOT EXISTS linkedin_profiles (id INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT, name TEXT, url TEXT)''')
            self.conn.execute('''CREATE TABLE IF NOT EXISTS sync_log (email TEXT, channel_id TEXT, start_date TEXT, end_date TEXT, last_synced INTEGER, PRIMARY KEY(email, channel_id, start_date, end_date))''')
            
            # Add indexes for frequently queried columns
            self.conn.execute('''CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp)''')
            self.conn.execute('''CREATE INDEX IF NOT EXISTS idx_messages_channel_id ON messages(channel_id)''')
            self.conn.execute('''CREATE INDEX IF NOT EXISTS idx_messages_thread_ts ON messages(thread_ts)''')
            self.conn.execute('''CREATE INDEX IF NOT EXISTS idx_messages_user_id ON messages(user_id)''')
            self.conn.execute('''CREATE INDEX IF NOT EXISTS idx_sync_log_last_synced ON sync_log(last_synced)''')

    def fetch_all_channels(self):
        all_channels = []
        cursor = None
        while True:
            try:
                result = self.client.conversations_list(types="public_channel,private_channel", limit=200, cursor=cursor)
                channels = result["channels"]
                all_channels.extend([{
                    'id': c['id'],
                    'name': c['name'],
                    'is_member': c.get('is_member', False),
                    'is_archived': c.get('is_archived', False)
                } for c in channels])
                cursor = result.get('response_metadata', {}).get('next_cursor')
                if not cursor:
                    break
            except SlackApiError as e:
                logger.error(f"Error fetching channels: {e}")
                if e.response['error'] == 'ratelimited':
                    retry_after = int(e.response.headers.get('Retry-After', 1))
                    time.sleep(retry_after)
                    continue
                raise  # Re-raise other Slack API errors
            except Exception as e:
                logger.error(f"Unexpected error fetching channels: {e}")
                raise
        return all_channels

    def fetch_all_users(self):
        all_users = {}
        cursor = None
        while True:
            try:
                result = self.client.users_list(cursor=cursor, limit=200)
                for user in result["members"]:
                    if not user.get("is_bot", False) and not user.get("deleted", False):
                        all_users[user["id"]] = {
                            "name": user.get("real_name", "Unknown"),
                            "username": user.get("name", "unknown"),
                            "email": user.get("profile", {}).get("email", "")
                        }
                cursor = result.get('response_metadata', {}).get('next_cursor')
                if not cursor:
                    break
            except SlackApiError as e:
                logging.error(f"Error fetching users: {e}")
                break
        return all_users

    def fetch_and_store_channel_messages(self, channel_id, channel_name, start_ts, end_ts, status_callback=None, user_email=None, users=None):
        all_messages = []
        cursor = None
        user_id = None
        if user_email and users:
            for uid, udata in users.items():
                if udata.get('email', '').lower() == user_email.lower():
                    user_id = uid
                    break
        while True:
            try:
                time.sleep(0.5)  # avoid rate limits
                result = self.client.conversations_history(
                    channel=channel_id,
                    oldest=str(start_ts),
                    latest=str(end_ts),
                    limit=200,
                    cursor=cursor
                )
                messages = result.get('messages', [])
                if user_id:
                    messages = [m for m in messages if m.get('user') == user_id]
                all_messages.extend(messages)
                cursor = result.get('response_metadata', {}).get('next_cursor')
                if not cursor:
                    break
            except SlackApiError as e:
                if status_callback:
                    status_callback(f"Error fetching messages for {channel_name}: {e.response['error']}")
                break
            except Exception as e:
                if status_callback:
                    status_callback(f"Unexpected error: {str(e)}")
                break
        # Store all messages, including thread replies
        self.store_messages(all_messages, channel_id, channel_name)

        # Collect all messages and thread replies for embedding
        def flatten_messages(messages):
            flat = []
            for msg in messages:
                flat.append(msg)
                # Fetch thread replies if this is a thread parent
                if msg.get('thread_ts') and msg.get('thread_ts') == msg.get('ts'):
                    try:
                        replies = self.client.conversations_replies(channel=channel_id, ts=msg['ts']).get('messages', [])
                        # Exclude the parent message (first in replies)
                        for reply in replies[1:]:
                            flat.append(reply)
                    except Exception as e:
                        if status_callback:
                            status_callback(f"[WARN] Could not fetch thread replies for {msg.get('ts')}: {e}")
            return flat

        all_flat = flatten_messages(all_messages)
        # Embed and index messages for semantic search
        if self.rag_backend and all_flat:
            try:
                self.rag_backend.index_slack_messages(all_flat, channel_name)
                if status_callback:
                    status_callback(f"Embedded and indexed {len(all_flat)} messages (including threads) for {channel_name}")
            except Exception as e:
                if status_callback:
                    status_callback(f"[ERROR] Embedding/indexing failed for {channel_name}: {e}")
        if status_callback:
            status_callback(f"Stored {len(all_messages)} messages for {channel_name}")

        # Ensure all parent messages for thread replies are present
        parent_ts_needed = set()
        for msg in all_messages:
            if msg.get('thread_ts') and msg.get('thread_ts') != msg.get('ts'):
                parent_ts_needed.add(msg['thread_ts'])

        existing_parent_ts = {msg['ts'] for msg in all_messages if msg.get('ts')}
        missing_parents = parent_ts_needed - existing_parent_ts

        for parent_ts in missing_parents:
            try:
                parent_result = self.client.conversations_replies(channel=channel_id, ts=parent_ts, limit=1)
                parent_msgs = parent_result.get('messages', [])
                if parent_msgs:
                    parent_msg = parent_msgs[0]
                    parent_msg['channel_id'] = channel_id
                    all_messages.append(parent_msg)
            except Exception as e:
                if status_callback:
                    status_callback(f"[WARN] Could not fetch parent message for thread {parent_ts} in channel {channel_id}: {e}")

    def store_messages(self, messages, channel_id, channel_name, batch_size=100):
        """Store messages in batches to avoid memory issues."""
        with self.conn:
            for i in range(0, len(messages), batch_size):
                batch = messages[i:i + batch_size]
                try:
                    for msg in batch:
                        message_id = f"{channel_id}_{msg['ts']}"
                        self.conn.execute(
                            '''INSERT OR REPLACE INTO messages (id, channel_id, channel_name, user_id, timestamp, datetime, text, thread_ts, is_thread_parent, has_linkedin_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                            (
                                message_id,
                                channel_id,
                                channel_name,
                                msg.get('user', ''),
                                float(msg['ts']),
                                datetime.fromtimestamp(float(msg['ts']), self.timezone).strftime('%Y-%m-%d %H:%M:%S'),
                                msg.get('text', ''),
                                msg.get('thread_ts', ''),
                                bool(msg.get('thread_ts') and msg.get('thread_ts') == msg.get('ts')),
                                bool(re.search(r'(?:https?://)?(?:www\.)?linkedin\.com/in/([^>\s|]+)', msg.get('text', '')))
                            )
                        )
                except Exception as e:
                    logger.error(f"Error storing message batch: {e}")
                    # Continue with next batch instead of failing completely
                    continue

    def fetch_thread_replies(self, channel_id, thread_ts, status_callback=None):
        """Fetch thread replies with proper error handling and rate limiting."""
        try:
            time.sleep(0.5)  # Rate limiting
            replies = self.client.conversations_replies(
                channel=channel_id,
                ts=thread_ts
            )
            if replies['ok']:
                return replies['messages'][1:]  # Skip parent message
            return []
        except SlackApiError as e:
            if e.response['error'] == 'ratelimited':
                retry_after = int(e.response.headers.get('Retry-After', 1))
                time.sleep(retry_after)
                return self.fetch_thread_replies(channel_id, thread_ts, status_callback)
            if status_callback:
                status_callback(f"[WARN] Could not fetch thread replies for {thread_ts}: {e.response['error']}")
            return []
        except Exception as e:
            if status_callback:
                status_callback(f"[WARN] Unexpected error fetching thread replies for {thread_ts}: {e}")
            return []

    def is_data_cached(self, email, channel_ids, start_date, end_date):
        # Check if all channels for this email and date range are cached
        with self.conn:
            for channel_id in channel_ids:
                cur = self.conn.execute('''SELECT last_synced FROM sync_log WHERE email=? AND channel_id=? AND start_date=? AND end_date=?''', (email, channel_id, str(start_date), str(end_date)))
                row = cur.fetchone()
                if not row:
                    return False
        return True

    def update_sync_log(self, email, channel_ids, start_date, end_date):
        now = int(time.time())
        with self.conn:
            for channel_id in channel_ids:
                self.conn.execute('''INSERT OR REPLACE INTO sync_log (email, channel_id, start_date, end_date, last_synced) VALUES (?, ?, ?, ?, ?)''', (email, channel_id, str(start_date), str(end_date), now))

    def sync_with_api(self, days=7, status_callback=None, channel_id=None, user_email=None, start_date=None, end_date=None, force_refresh=False, fetch_all_messages=False):
        print(f"[DEBUG] sync_with_api called with force_refresh={force_refresh}")
        if status_callback:
            status_callback('Starting sync...')

        # Convert dates to timestamps
        if start_date and end_date:
            start_ts = datetime.combine(start_date, datetime.min.time()).timestamp()
            end_ts = datetime.combine(end_date, datetime.max.time()).timestamp()
        else:
            end_ts = time.time()
            start_ts = end_ts - (days * 24 * 60 * 60)

        if not channel_id:
            channels = self.fetch_all_channels()
            # Only include channels whose names contain 'candidatelabs' or 'candidate-labs'
            filtered_channels = [
                c for c in channels
                if (
                    'candidate-labs' in c['name'].lower() or
                    'candidatelabs' in c['name'].lower()
                )
            ]
            # Loop over filtered_channels directly
            for channel in filtered_channels:
                ch_id = channel['id']
                if status_callback:
                    status_callback(f'Fetching messages for channel {ch_id}...')
                all_messages = []
                cursor = None
                while True:
                    result = self.client.conversations_history(
                        channel=ch_id,
                        oldest=str(start_ts),
                        latest=str(end_ts),
                        limit=200,
                        cursor=cursor
                    )
                    messages = result.get('messages', [])
                    for msg in messages:
                        msg['channel_id'] = ch_id
                    all_messages.extend(messages)
                    cursor = result.get('response_metadata', {}).get('next_cursor')
                    if not cursor:
                        break
                # For each thread parent, fetch ALL replies (regardless of reply timestamp)
                thread_replies = []
                for msg in all_messages:
                    if msg.get('thread_ts') and msg['thread_ts'] == msg['ts']:
                        reply_cursor = None
                        while True:
                            replies_result = self.client.conversations_replies(
                                channel=ch_id,
                                ts=msg['ts'],
                                limit=200,
                                cursor=reply_cursor
                            )
                            replies = replies_result.get('messages', [])
                            for reply in replies[1:]:
                                reply['channel_id'] = ch_id
                            thread_replies.extend(replies[1:])
                            reply_cursor = replies_result.get('response_metadata', {}).get('next_cursor')
                            if not reply_cursor:
                                break
                all_messages.extend(thread_replies)
                # Ensure all parent messages for thread replies are present
                parent_ts_needed = set()
                for msg in all_messages:
                    if msg.get('thread_ts') and msg.get('thread_ts') != msg.get('ts'):
                        parent_ts_needed.add(msg['thread_ts'])
                existing_parent_ts = {msg['ts'] for msg in all_messages if msg.get('ts')}
                missing_parents = parent_ts_needed - existing_parent_ts
                for parent_ts in missing_parents:
                    try:
                        parent_result = self.client.conversations_replies(channel=ch_id, ts=parent_ts, limit=1)
                        parent_msgs = parent_result.get('messages', [])
                        if parent_msgs:
                            parent_msg = parent_msgs[0]
                            parent_msg['channel_id'] = ch_id
                            all_messages.append(parent_msg)
                    except Exception as e:
                        if status_callback:
                            status_callback(f"[WARN] Could not fetch parent message for thread {parent_ts} in channel {ch_id}: {e}")
                self.store_messages(all_messages, ch_id, channel['name'])
                if status_callback:
                    status_callback(f'Stored {len(all_messages)} messages for {ch_id}')
            if status_callback:
                status_callback('Sync complete!')
            return True
        else:
            # If a specific channel_id is provided, fallback to previous logic
            ch_id = channel_id
            if status_callback:
                status_callback(f'Fetching messages for channel {ch_id}...')
            all_messages = []
            cursor = None
            while True:
                result = self.client.conversations_history(
                    channel=ch_id,
                    oldest=str(start_ts),
                    latest=str(end_ts),
                    limit=200,
                    cursor=cursor
                )
                messages = result.get('messages', [])
                for msg in messages:
                    msg['channel_id'] = ch_id
                all_messages.extend(messages)
                cursor = result.get('response_metadata', {}).get('next_cursor')
                if not cursor:
                    break
            thread_replies = []
            for msg in all_messages:
                if msg.get('thread_ts') and msg['thread_ts'] == msg['ts']:
                    reply_cursor = None
                    while True:
                        replies_result = self.client.conversations_replies(
                            channel=ch_id,
                            ts=msg['ts'],
                            limit=200,
                            cursor=reply_cursor
                        )
                        replies = replies_result.get('messages', [])
                        for reply in replies[1:]:
                            reply['channel_id'] = ch_id
                        thread_replies.extend(replies[1:])
                        reply_cursor = replies_result.get('response_metadata', {}).get('next_cursor')
                        if not reply_cursor:
                            break
            all_messages.extend(thread_replies)
            parent_ts_needed = set()
            for msg in all_messages:
                if msg.get('thread_ts') and msg.get('thread_ts') != msg.get('ts'):
                    parent_ts_needed.add(msg['thread_ts'])
            existing_parent_ts = {msg['ts'] for msg in all_messages if msg.get('ts')}
            missing_parents = parent_ts_needed - existing_parent_ts
            for parent_ts in missing_parents:
                try:
                    parent_result = self.client.conversations_replies(channel=ch_id, ts=parent_ts, limit=1)
                    parent_msgs = parent_result.get('messages', [])
                    if parent_msgs:
                        parent_msg = parent_msgs[0]
                        parent_msg['channel_id'] = ch_id
                        all_messages.append(parent_msg)
                except Exception as e:
                    if status_callback:
                        status_callback(f"[WARN] Could not fetch parent message for thread {parent_ts} in channel {ch_id}: {e}")
            self.store_messages(all_messages, ch_id, ch_id)  # fallback: use channel id as name
            if status_callback:
                status_callback(f'Stored {len(all_messages)} messages for {ch_id}')
            if status_callback:
                status_callback('Sync complete!')
            return True

    def store_users(self, users):
        current_time = int(time.time())
        with self.conn:
            for user_id, user_data in users.items():
                self.conn.execute(
                    '''INSERT OR REPLACE INTO users (id, name, username, email, last_updated) VALUES (?, ?, ?, ?, ?)''',
                    (user_id, user_data['name'], user_data['username'], user_data.get('email', ''), current_time)
                )

    def store_channels(self, channels):
        current_time = int(time.time())
        with self.conn:
            for channel in channels:
                self.conn.execute(
                    '''INSERT OR REPLACE INTO channels (id, name, is_member, is_archived, last_updated) VALUES (?, ?, ?, ?, ?)''',
                    (channel['id'], channel['name'], channel['is_member'], channel['is_archived'], current_time)
                )

    def _extract_client_name(self, channel_name: str) -> Optional[str]:
        """Extract client name from channel name."""
        match = self.client_channel_pattern.search(channel_name)
        if match:
            # Get the first capture group and capitalize it
            client_name = match.group(1).capitalize()
            return client_name
        return None

    def _is_client_channel(self, channel_name: str) -> bool:
        """Check if a channel is a client channel."""
        return bool(self.client_channel_pattern.search(channel_name))

    @log_metrics
    def get_channels(self) -> list[dict]:
        """Get list of client channels with extracted client names."""
        try:
            with self.conn:
                cursor = self.conn.execute("SELECT id, name FROM channels WHERE is_member = 1 AND is_archived = 0")
                channels = [{'id': row[0], 'name': row[1]} for row in cursor.fetchall()]
                client_channels = []
                for c in channels:
                    if self._is_client_channel(c['name']):
                        client_name = self._extract_client_name(c['name']) or c['name']
                        client_channels.append({
                            'id': c['id'],
                            'name': client_name,
                            'original_name': c['name']
                        })
                return client_channels
        except Exception as e:
            logger.error("Failed to get client channels", error=str(e))
            return []

    def claude_prompt(self, prompt, channel_id=None, start_date=None, end_date=None, limit=50):
        # Use RAG backend if available
        if self.rag_backend:
            from datetime import datetime
            import pytz
            tz = pytz.timezone("America/Chicago")
            if start_date and end_date:
                start_dt = datetime.strptime(str(start_date), '%Y-%m-%d').replace(tzinfo=tz)
                end_dt = datetime.strptime(str(end_date), '%Y-%m-%d').replace(tzinfo=tz)
                start_ts = start_dt.timestamp()
                end_ts = end_dt.timestamp()
            else:
                start_ts = None
                end_ts = None

            # Use candidate-anchored context for candidate listing/summarization prompts
            candidate_keywords = ["list candidates", "summarize candidates", "candidate feedback", "candidates posted", "client feedback", "status of candidates"]
            if any(kw in prompt.lower() for kw in candidate_keywords):
                context = self.rag_backend.build_claude_context_by_candidate(
                    start_ts=start_ts,
                    end_ts=end_ts,
                    channel_id=channel_id
                )
            else:
                context = self.rag_backend.build_claude_context_with_all_thread_replies(
                    query=prompt,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    limit=limit
                )
            full_prompt = f"""Here is the context from Slack messages and threads:\n\n{context}\n\nNow, please respond to this query:\n{prompt}"""
            response = self.rag_backend.claude_completion(full_prompt)
            return response
        return f"Query: {prompt}\n\nNo RAG backend available for enhanced context."

    def search_messages(self, query, channel_id=None, start_date=None, end_date=None, limit=100):
        params = [f"%{query}%"]
        sql_query = """
            SELECT m.id, m.channel_id, m.channel_name, m.user_id, m.timestamp, m.datetime, m.text
            FROM messages m
            WHERE m.text LIKE ?
        """
        if channel_id:
            sql_query += " AND m.channel_id = ?"
            params.append(channel_id)
        if start_date:
            start_ts = datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=self.timezone).timestamp()
            sql_query += " AND m.timestamp >= ?"
            params.append(start_ts)
        if end_date:
            end_ts = datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=self.timezone).timestamp()
            sql_query += " AND m.timestamp <= ?"
            params.append(end_ts)
        sql_query += " ORDER BY m.timestamp DESC LIMIT ?"
        params.append(limit)
        with self.conn:
            cursor = self.conn.execute(sql_query, params)
            results = []
            for row in cursor.fetchall():
                results.append({
                    'id': row[0],
                    'channel_id': row[1],
                    'channel_name': row[2],
                    'user_id': row[3],
                    'timestamp': row[4],
                    'datetime': row[5],
                    'text': row[6],
                })
            return results

    def get_messages_by_date_range(self, start_ts, end_ts, channel_id=None):
        # Delegate to the data store (assumes self.data_store exists)
        return self.data_store.get_messages_by_date_range(start_ts, end_ts, channel_id)

    def get_cached_messages(self, start_ts, end_ts, channel_id=None):
        """Get messages from cache within the specified time range."""
        try:
            params = [start_ts, end_ts]
            query = """
                SELECT m.*, u.name as user_name, u.username as user_username
                FROM messages m
                LEFT JOIN users u ON m.user_id = u.id
                WHERE m.timestamp >= ? AND m.timestamp <= ?
            """
            if channel_id:
                query += " AND m.channel_id = ?"
                params.append(channel_id)
            query += " ORDER BY m.timestamp DESC"
            
            with self.conn:
                cursor = self.conn.execute(query, params)
                messages = []
                for row in cursor.fetchall():
                    message = {
                        'id': row[0],
                        'channel_id': row[1],
                        'channel_name': row[2],
                        'user': row[3],
                        'user_name': row[10] or 'Unknown User',
                        'user_username': row[11] or 'unknown',
                        'ts': str(row[4]),
                        'datetime': row[5],
                        'text': row[6],
                        'thread_ts': row[7],
                        'is_thread_parent': bool(row[8]),
                        'has_linkedin_url': bool(row[9])
                    }
                    messages.append(message)
                return messages
        except Exception as e:
            logger.error(f"Error getting cached messages: {e}")
            return None

    # Placeholder for search and Claude prompt methods
    # ... 