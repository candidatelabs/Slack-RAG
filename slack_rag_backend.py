import os
import openai
import chromadb
from chromadb.config import Settings
from chromadb.utils import embedding_functions
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime
import pytz
from dotenv import load_dotenv
import anthropic
from config import APIConfig
from candidate_extractor import CandidateExtractor
import time
from http.client import IncompleteRead
from anthropic import Anthropic

# Load environment variables
load_dotenv()

def safe_slack_api_call(api_func, *args, **kwargs):
    retries = 3
    for attempt in range(retries):
        try:
            return api_func(*args, **kwargs)
        except IncompleteRead as e:
            print(f"[WARN] IncompleteRead: {e}. Retrying ({attempt+1}/{retries})...")
            time.sleep(2)
    raise RuntimeError("Failed after retries due to IncompleteRead")

class SlackRAGBackend:
    def __init__(self, chroma_path=".chroma", openai_api_key=None, slack_token=None, timezone="America/Chicago", data_store=None):
        self.chroma_path = chroma_path
        self.openai_api_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        self.slack_token = slack_token or os.getenv("SLACK_TOKEN")
        self.timezone = pytz.timezone(timezone)
        self.data_store = data_store
        if not self.openai_api_key:
            raise RuntimeError("OPENAI_API_KEY not set")
        if not self.slack_token:
            raise RuntimeError("SLACK_TOKEN not set")
        openai.api_key = self.openai_api_key
        self.client = WebClient(token=self.slack_token)
        self.chroma_client = chromadb.Client(Settings(persist_directory=self.chroma_path))
        self.collection = self.chroma_client.get_or_create_collection(
            name="slack-messages",
            embedding_function=embedding_functions.OpenAIEmbeddingFunction(api_key=self.openai_api_key, model_name="text-embedding-3-small")
        )
        self.claude = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    def index_slack_messages(self, messages, channel_name):
        print(f"[DEBUG] Embedding {len(messages)} messages for channel {channel_name} (OpenAI API should be called)")
        candidate_extractor = CandidateExtractor()
        # Extract all candidates from the batch of messages for this channel
        candidates = candidate_extractor.extract_candidates(messages, channel_name)
        print(f"[DEBUG] Extracted candidates: {candidates}")
        # Group messages by candidate (using LinkedIn URL as unique key)
        candidate_messages = {c['linkedin_url']: [] for c in candidates}
        for msg in messages:
            text = msg.get('text', '')
            for c in candidates:
                if c['name'] in text or c['linkedin_url'] in text:
                    candidate_messages[c['linkedin_url']].append(msg)
        # Index messages for each candidate
        for candidate_url, msgs in candidate_messages.items():
            if not msgs:
                continue
            docs = []
            metadatas = []
            ids = []
            candidate = next((c for c in candidates if c['linkedin_url'] == candidate_url), None)
            def get_main_parent_text(msg, all_msgs):
                parent_ts = msg.get('thread_ts')
                if not parent_ts or msg.get('ts') == parent_ts:
                    return msg.get('text', '')
                parent = next((m for m in all_msgs if m.get('ts') == parent_ts), None)
                if parent:
                    return get_main_parent_text(parent, all_msgs)
                return "[Parent: unknown]"

            for msg in msgs:
                dt = datetime.fromtimestamp(float(msg['ts']), self.timezone).strftime('%Y-%m-%d %H:%M:%S')
                # For any thread reply, prepend the main parent message's text
                if msg.get('thread_ts') and msg.get('thread_ts') != msg.get('ts'):
                    main_parent_text = get_main_parent_text(msg, msgs)
                    doc = f"{dt} [{channel_name}] (thread reply) [Main message: {main_parent_text}] {msg.get('text', '')}"
                else:
                    doc = f"{dt} [{channel_name}] {msg.get('text', '')}"
                docs.append(doc)
                metadatas.append({
                    "channel": channel_name,
                    "user": msg.get('user', ''),
                    "ts": msg['ts'],
                    "datetime": dt,
                    "candidate": candidate['name'] if candidate else '',
                    "linkedin_url": candidate_url,
                    "is_thread_reply": bool(msg.get('thread_ts') and msg.get('thread_ts') != msg.get('ts'))
                })
                ids.append(f"{channel_name}_{msg['ts']}_{candidate_url}")
            if docs:
                self.collection.add(documents=docs, metadatas=metadatas, ids=ids)

    def optimize_query_with_claude(self, query):
        """
        Optimize the query using Claude to get a more precise or expanded version.
        """
        system_prompt = (
            "You are an assistant that helps optimize search queries. "
            "Provide a more precise or expanded version of the given query to improve search results."
        )
        user_prompt = f"Optimize the following query for better search results: {query}"
        claude = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        try:
            response = claude.messages.create(
                model="claude-3-7-sonnet-20250219",
                max_tokens=2000,
                system=system_prompt,
                messages=[
                    {"role": "user", "content": user_prompt}
                ]
            )
            return response.content[0].text
        except Exception as e:
            return f"[ERROR] Claude API error: {str(e)}"

    def semantic_search(self, query, n_results=10, channel=None, start_date=None, end_date=None):
        # Optimize the query using Claude
        optimized_query = self.optimize_query_with_claude(query)
        print(f"[DEBUG] Optimized query: {optimized_query}")
        # Optionally filter by channel and date
        where = {}
        if channel:
            where["channel"] = channel
        if start_date:
            where["datetime"] = {"$gte": start_date}
        if end_date:
            where.setdefault("datetime", {})["$lte"] = end_date
        if where:
            results = self.collection.query(query_texts=[optimized_query], n_results=n_results, where=where)
        else:
            results = self.collection.query(query_texts=[optimized_query], n_results=n_results)
        return results["documents"][0] if results["documents"] else []

    def build_claude_context(self, query, n_results=10, channel=None, start_date=None, end_date=None):
        top_docs = self.semantic_search(query, n_results, channel, start_date, end_date)
        context = "\n".join(top_docs)
        return context

    def ask_claude(self, user_prompt, n_results=10, channel=None, start_date=None, end_date=None):
        """
        Build a RAG context from semantic search and ask Claude with a system prompt and user prompt.
        """
        # Build context from semantic search
        context = self.build_claude_context(user_prompt, n_results, channel, start_date, end_date)
        # System prompt
        system_prompt = (
            "You are an assistant that answers questions strictly based on the following Slack messages. "
            "Do not use outside knowledge. If the answer is not in the messages, say so.\n\n"
            f"Slack messages:\n{context}"
        )
        # User prompt
        user_prompt = user_prompt.strip()
        # Claude client
        anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
        if not anthropic_api_key:
            raise RuntimeError("ANTHROPIC_API_KEY not set")
        claude = anthropic.Anthropic(api_key=anthropic_api_key)
        try:
            response = claude.messages.create(
                model="claude-3-7-sonnet-20250219",
                max_tokens=2000,
                system=system_prompt,
                messages=[
                    {"role": "user", "content": user_prompt}
                ]
            )
            return response.content[0].text
        except Exception as e:
            return f"[ERROR] Claude API error: {str(e)}"

    # Example: use this context in your Claude prompt
    # def ask_claude(self, user_prompt, context, claude_client):
    #     ... 

    def get_recent_thread_replies_with_parent(self, start_ts, end_ts, channel_id=None):
        """
        Returns a list of dicts: each with 'reply' (the thread reply message) and 'parent' (the main message).
        Only includes thread replies in the date range, regardless of parent message date.
        """
        # Get all messages in the channel/date range
        all_msgs = self.data_store.get_messages_by_date_range(0, end_ts, channel_id)  # 0 to end_ts to get all possible parents
        # Build a lookup for parent messages by ts
        parent_lookup = {msg.get('ts', msg.get('timestamp')): msg for msg in all_msgs if msg.get('ts') or msg.get('timestamp')}
        # Get thread replies in the date range
        recent_replies = [
            msg for msg in all_msgs
            if msg.get('thread_ts') and (msg.get('ts') != msg.get('thread_ts'))
            and start_ts <= float(msg.get('ts', msg.get('timestamp', 0))) <= end_ts
        ]
        results = []
        for reply in recent_replies:
            parent = parent_lookup.get(reply.get('thread_ts'))
            results.append({'reply': reply, 'parent': parent})
        return results

    def _get_channel_name(self, channel_id):
        if not hasattr(self, '_channel_name_cache'):
            self._channel_name_cache = {}
        if channel_id in self._channel_name_cache:
            return self._channel_name_cache[channel_id]
        # Look up channel name from the database
        if hasattr(self.data_store, 'conn'):
            cur = self.data_store.conn.execute("SELECT name FROM channels WHERE id=?", (channel_id,))
            row = cur.fetchone()
            if row:
                self._channel_name_cache[channel_id] = row[0]
                return row[0]
        return channel_id

    def _get_user_name(self, user_id):
        if not hasattr(self, '_user_name_cache'):
            self._user_name_cache = {}
        if user_id in self._user_name_cache:
            return self._user_name_cache[user_id]
        # Look up user name from the database
        if hasattr(self.data_store, 'conn'):
            cur = self.data_store.conn.execute("SELECT name FROM users WHERE id=?", (user_id,))
            row = cur.fetchone()
            if row:
                self._user_name_cache[user_id] = row[0]
                return row[0]
        return user_id or 'Unknown User'

    def build_claude_context_by_candidate(self, start_ts=None, end_ts=None, channel_id=None):
        """
        Build context for Claude grouped by channel (client), with each candidate submission listed under the channel.
        Format: Channel header, then candidate name, submission date, feedback, and status/next action.
        """
        messages = self.data_store.get_messages_by_date_range(start_ts or 0, end_ts or float('inf'), channel_id)
        candidate_extractor = CandidateExtractor()
        # Group candidate submissions by channel
        channel_candidates = {}
        for msg in messages:
            candidates = candidate_extractor.extract_candidates(msg.get('text', ''))
            if not candidates:
                continue
            channel_name = msg.get('channel_name') or self._get_channel_name(msg.get('channel_id'))
            user_name = self._get_user_name(msg.get('user'))
            submission_date = msg.get('datetime', msg.get('ts', ''))
            thread_ts = msg.get('thread_ts', msg.get('ts'))
            replies = [m for m in messages if m.get('thread_ts') == thread_ts and m.get('ts') != thread_ts]
            feedbacks = []
            for reply in replies:
                reply_user = self._get_user_name(reply.get('user'))
                feedbacks.append(f'"{reply.get("text", "")}" (by {reply_user})')
            for candidate in candidates:
                candidate_block = f"- {candidate['name']} - submitted {submission_date}"
                if feedbacks:
                    candidate_block += f"\n  feedback: {feedbacks[0]}"
                    if len(feedbacks) > 1:
                        for fb in feedbacks[1:]:
                            candidate_block += f"\n  additional feedback: {fb}"
                    candidate_block += "\n  status: (Claude, please infer status from feedback above)"
                else:
                    candidate_block += "\n  no feedback from client"
                    candidate_block += "\n  status: Follow up with client to see if they're interested."
                if channel_name not in channel_candidates:
                    channel_candidates[channel_name] = []
                channel_candidates[channel_name].append(candidate_block)
        # Build the context string
        context_blocks = []
        for channel, candidates in channel_candidates.items():
            context_blocks.append(f"{channel}")
            context_blocks.extend(candidates)
        return "\n\n".join(context_blocks)

    def build_claude_context_with_all_thread_replies(self, query, start_ts=None, end_ts=None, limit=50, channel_id=None):
        """
        Build context for Claude that includes all thread replies from the specified time range.
        If no time range is specified, uses all available messages.
        """
        messages = self.data_store.get_messages_by_date_range(start_ts or 0, end_ts or float('inf'), channel_id)
        thread_messages = {}
        for msg in messages:
            thread_ts = msg.get('thread_ts', msg.get('ts'))
            if thread_ts not in thread_messages:
                thread_messages[thread_ts] = []
            thread_messages[thread_ts].append(msg)
        context_blocks = []
        semantic_results = self.semantic_search(query, n_results=limit)
        if semantic_results:
            context_blocks.append("=== Semantic Search Results ===")
            for result in semantic_results:
                channel_name = result.get('channel_name') or self._get_channel_name(result.get('channel_id'))
                user_name = self._get_user_name(result.get('user'))
                context_blocks.append(f"Message: {result['text']}")
                context_blocks.append(f"Channel: {channel_name}")
                context_blocks.append(f"User: {user_name}")
                context_blocks.append(f"Timestamp: {datetime.fromtimestamp(float(result.get('ts', 0))).strftime('%Y-%m-%d %H:%M:%S')}")
                context_blocks.append("---")
        context_blocks.append("\n=== Thread Replies ===")
        for thread_ts, thread_msgs in thread_messages.items():
            thread_msgs.sort(key=lambda x: float(x.get('ts', 0)))
            parent_msg = next((msg for msg in thread_msgs if msg.get('ts') == thread_ts), None)
            if not parent_msg:
                continue
            channel_name = parent_msg.get('channel_name') or self._get_channel_name(parent_msg.get('channel_id'))
            user_name = self._get_user_name(parent_msg.get('user'))
            context_blocks.append(f"\nThread started by: {user_name}")
            context_blocks.append(f"Parent message: {parent_msg.get('text', '')}")
            context_blocks.append(f"Channel: {channel_name}")
            context_blocks.append(f"Timestamp: {datetime.fromtimestamp(float(thread_ts)).strftime('%Y-%m-%d %H:%M:%S')}")
            replies = [msg for msg in thread_msgs if msg.get('ts') != thread_ts]
            if replies:
                context_blocks.append("\nReplies:")
                for reply in replies:
                    reply_user = self._get_user_name(reply.get('user'))
                    context_blocks.append(f"- {reply_user}: {reply.get('text', '')}")
                    context_blocks.append(f"  {datetime.fromtimestamp(float(reply.get('ts', 0))).strftime('%Y-%m-%d %H:%M:%S')}")
            context_blocks.append("---")
        return "\n".join(context_blocks)

    def claude_completion(self, prompt):
        """
        Send a prompt to Claude and get the response.
        """
        try:
            response = self.claude.messages.create(
                model="claude-3-7-sonnet-20250219",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text
        except Exception as e:
            return f"[ERROR] Claude API error: {str(e)}"

    def build_claude_context_by_candidate(self, start_ts=None, end_ts=None, channel_id=None):
        """
        Build context for Claude anchored to each candidate submission (message with LinkedIn hyperlink),
        including all thread replies for each submission. No deduplication by LinkedIn URL.
        """
        messages = self.data_store.get_messages_by_date_range(start_ts or 0, end_ts or float('inf'), channel_id)
        candidate_extractor = CandidateExtractor()
        candidate_blocks = []
        for msg in messages:
            candidates = candidate_extractor.extract_candidates(msg.get('text', ''))
            if not candidates:
                continue
            channel_name = msg.get('channel_name') or self._get_channel_name(msg.get('channel_id'))
            user_name = self._get_user_name(msg.get('user'))
            for candidate in candidates:
                block = [f"Candidate: {candidate['name']} ({candidate['linkedin_url']})"]
                block.append(f"Submission: {msg.get('text', '')}")
                block.append(f"Channel: {channel_name}")
                block.append(f"Submitted by: {user_name}")
                block.append(f"Timestamp: {msg.get('datetime', msg.get('ts', ''))}")
                thread_ts = msg.get('thread_ts', msg.get('ts'))
                replies = [m for m in messages if m.get('thread_ts') == thread_ts and m.get('ts') != thread_ts]
                if replies:
                    block.append("Feedback/Updates:")
                    for reply in replies:
                        reply_user = self._get_user_name(reply.get('user'))
                        block.append(f"- {reply_user}: {reply.get('text', '')} [{reply.get('datetime', reply.get('ts', ''))}]")
                candidate_blocks.append("\n".join(block) + "\n---")
        return "\n\n".join(candidate_blocks) 