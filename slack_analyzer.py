#!/usr/bin/env python3

import os
import sys
from datetime import datetime, timedelta
import pytz
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import pandas as pd
from dotenv import load_dotenv
import re
import argparse
from typing import Dict, List, Tuple, Optional, Any
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import anthropic
import cmd
import readline
import pickle
import sqlite3
import time
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich.text import Text
import logging

# Load environment variables
load_dotenv()

# Set up argument parser
parser = argparse.ArgumentParser(description='Interactive Slack Data Analyzer')
parser.add_argument('--debug', action='store_true', help='Enable debug logging')
args = parser.parse_args()

# Configure logging based on debug flag
if args.debug:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)

class SlackDataStore:
    """Manages storage and retrieval of Slack data."""
    
    def __init__(self, db_path: str = '.slack_data.db'):
        """Initialize the data store with the database path."""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.create_tables()
        
    def create_tables(self):
        """Create necessary tables if they don't exist."""
        with self.conn:
            # Channels table
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS channels (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    is_member BOOLEAN,
                    is_archived BOOLEAN,
                    last_updated INTEGER
                )
            ''')
            
            # Users table
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    username TEXT,
                    email TEXT,
                    last_updated INTEGER
                )
            ''')
            
            # Messages table
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS messages (
                    id TEXT PRIMARY KEY,
                    channel_id TEXT,
                    channel_name TEXT,
                    user_id TEXT,
                    timestamp REAL,
                    datetime TEXT,
                    text TEXT,
                    thread_ts TEXT,
                    is_thread_parent BOOLEAN,
                    has_linkedin_url BOOLEAN,
                    FOREIGN KEY (channel_id) REFERENCES channels(id),
                    FOREIGN KEY (user_id) REFERENCES users(id)
                )
            ''')
            
            # LinkedIn profiles table
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS linkedin_profiles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id TEXT,
                    name TEXT,
                    url TEXT,
                    FOREIGN KEY (message_id) REFERENCES messages(id)
                )
            ''')
            
            # Create indices for faster queries
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel_id)')
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp)')
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_messages_thread ON messages(thread_ts)')
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_linkedin_message ON linkedin_profiles(message_id)')
    
    def store_channels(self, channels: List[Dict]):
        """Store channel information."""
        current_time = int(time.time())
        with self.conn:
            for channel in channels:
                self.conn.execute(
                    '''
                    INSERT OR REPLACE INTO channels (id, name, is_member, is_archived, last_updated)
                    VALUES (?, ?, ?, ?, ?)
                    ''',
                    (channel['id'], channel['name'], channel['is_member'], channel['is_archived'], current_time)
                )
    
    def store_users(self, users: Dict[str, Dict]):
        """Store user information."""
        current_time = int(time.time())
        with self.conn:
            for user_id, user_data in users.items():
                self.conn.execute(
                    '''
                    INSERT OR REPLACE INTO users (id, name, username, email, last_updated)
                    VALUES (?, ?, ?, ?, ?)
                    ''',
                    (user_id, user_data['name'], user_data['username'], user_data.get('email', ''), current_time)
                )
    
    def store_messages(self, messages: List[Dict], channel_id: str, channel_name: str):
        """Store messages and associated LinkedIn profiles."""
        with self.conn:
            for msg in messages:
                # Create a unique ID for the message
                message_id = f"{channel_id}_{msg['ts']}"
                
                # Insert the message
                self.conn.execute(
                    '''
                    INSERT OR REPLACE INTO messages 
                    (id, channel_id, channel_name, user_id, timestamp, datetime, text, 
                     thread_ts, is_thread_parent, has_linkedin_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    (
                        message_id,
                        channel_id,
                        channel_name,
                        msg.get('user', ''),
                        float(msg['ts']),
                        msg.get('datetime', ''),
                        msg.get('text', ''),
                        msg.get('thread_ts', ''),
                        msg.get('is_thread_parent', False),
                        msg.get('has_linkedin_url', False)
                    )
                )
                
                # Insert LinkedIn profiles if any
                if msg.get('has_linkedin_url') and msg.get('linkedin_profiles'):
                    for profile in msg['linkedin_profiles']:
                        self.conn.execute(
                            '''
                            INSERT INTO linkedin_profiles (message_id, name, url)
                            VALUES (?, ?, ?)
                            ''',
                            (message_id, profile['name'], profile['url'])
                        )
                
                # Store thread messages if any
                if msg.get('is_thread_parent') and msg.get('thread_messages'):
                    self.store_messages(msg['thread_messages'], channel_id, channel_name)
    
    def get_channels(self, active_only: bool = True) -> List[Dict]:
        """Get channels from the database."""
        query = "SELECT id, name, is_member, is_archived FROM channels"
        if active_only:
            query += " WHERE is_member = 1 AND is_archived = 0"
        
        with self.conn:
            cursor = self.conn.execute(query)
            return [{'id': row[0], 'name': row[1], 'is_member': bool(row[2]), 'is_archived': bool(row[3])}
                    for row in cursor.fetchall()]
    
    def get_channel_by_name(self, name: str) -> Optional[Dict]:
        """Get a channel by name."""
        with self.conn:
            cursor = self.conn.execute(
                "SELECT id, name, is_member, is_archived FROM channels WHERE name = ? LIMIT 1",
                (name,)
            )
            row = cursor.fetchone()
            if row:
                return {'id': row[0], 'name': row[1], 'is_member': bool(row[2]), 'is_archived': bool(row[3])}
            return None
    
    def get_messages_by_date_range(self, start_ts: float, end_ts: float, channel_id: str = None) -> List[Dict]:
        """Get messages within a date range for a specific channel or all channels."""
        query = """
            SELECT m.id, m.channel_id, m.channel_name, m.user_id, u.name as user_name, u.username as user_username,
                   m.timestamp, m.datetime, m.text, m.thread_ts, m.is_thread_parent, m.has_linkedin_url
            FROM messages m
            LEFT JOIN users u ON m.user_id = u.id
            WHERE m.timestamp >= ? AND m.timestamp <= ?
        """
        params = [start_ts, end_ts]
        
        if channel_id:
            query += " AND m.channel_id = ?"
            params.append(channel_id)
        
        with self.conn:
            cursor = self.conn.execute(query, params)
            messages = []
            for row in cursor.fetchall():
                message = {
                    'id': row[0],
                    'channel_id': row[1],
                    'channel_name': row[2],
                    'user': row[3],
                    'user_name': row[4] or 'Unknown User',
                    'user_username': row[5] or 'unknown',
                    'ts': str(row[6]),
                    'datetime': row[7],
                    'text': row[8],
                    'thread_ts': row[9] if row[9] else None,
                    'is_thread_parent': bool(row[10]),
                    'has_linkedin_url': bool(row[11])
                }
                
                # Get LinkedIn profiles for this message
                if message['has_linkedin_url']:
                    linkedin_cursor = self.conn.execute(
                        "SELECT name, url FROM linkedin_profiles WHERE message_id = ?",
                        (message['id'],)
                    )
                    message['linkedin_profiles'] = [
                        {'name': lrow[0], 'url': lrow[1]} for lrow in linkedin_cursor.fetchall()
                    ]
                
                # Get thread messages if this is a thread parent
                if message['is_thread_parent']:
                    thread_cursor = self.conn.execute(
                        """
                        SELECT m.id, m.user_id, u.name as user_name, u.username as user_username,
                               m.timestamp, m.datetime, m.text, m.has_linkedin_url
                        FROM messages m
                        LEFT JOIN users u ON m.user_id = u.id
                        WHERE m.thread_ts = ? AND m.id != ?
                        ORDER BY m.timestamp
                        """,
                        (message['thread_ts'], message['id'])
                    )
                    thread_messages = []
                    for trow in thread_cursor.fetchall():
                        thread_msg = {
                            'id': trow[0],
                            'user': trow[1],
                            'user_name': trow[2] or 'Unknown User',
                            'user_username': trow[3] or 'unknown',
                            'ts': str(trow[4]),
                            'datetime': trow[5],
                            'text': trow[6],
                            'has_linkedin_url': bool(trow[7])
                        }
                        thread_messages.append(thread_msg)
                    
                    message['thread_messages'] = thread_messages
                    message['thread_count'] = len(thread_messages)
                
                messages.append(message)
            
            return messages
    
    def search_messages(self, query: str, channel_id: str = None, start_ts: float = None, end_ts: float = None) -> List[Dict]:
        """Search messages containing a specific query."""
        params = [f"%{query}%"]
        sql_query = """
            SELECT m.id, m.channel_id, m.channel_name, m.user_id, u.name as user_name, u.username as user_username,
                   m.timestamp, m.datetime, m.text, m.thread_ts, m.is_thread_parent, m.has_linkedin_url
            FROM messages m
            LEFT JOIN users u ON m.user_id = u.id
            WHERE m.text LIKE ?
        """
        
        if channel_id:
            sql_query += " AND m.channel_id = ?"
            params.append(channel_id)
        
        if start_ts:
            sql_query += " AND m.timestamp >= ?"
            params.append(start_ts)
        
        if end_ts:
            sql_query += " AND m.timestamp <= ?"
            params.append(end_ts)
            
        sql_query += " ORDER BY m.timestamp DESC LIMIT 100"
        
        with self.conn:
            cursor = self.conn.execute(sql_query, params)
            messages = []
            for row in cursor.fetchall():
                message = {
                    'id': row[0],
                    'channel_id': row[1],
                    'channel_name': row[2],
                    'user': row[3],
                    'user_name': row[4] or 'Unknown User',
                    'user_username': row[5] or 'unknown',
                    'ts': str(row[6]),
                    'datetime': row[7],
                    'text': row[8],
                    'thread_ts': row[9] if row[9] else None,
                    'is_thread_parent': bool(row[10]),
                    'has_linkedin_url': bool(row[11])
                }
                messages.append(message)
            
            return messages
    
    def get_linkedin_profiles(self, channel_id: str = None, start_ts: float = None, end_ts: float = None) -> List[Dict]:
        """Get LinkedIn profiles mentioned in messages."""
        query = """
            SELECT lp.name, lp.url, m.channel_name, m.datetime, m.text
            FROM linkedin_profiles lp
            JOIN messages m ON lp.message_id = m.id
            WHERE 1=1
        """
        params = []
        
        if channel_id:
            query += " AND m.channel_id = ?"
            params.append(channel_id)
        
        if start_ts:
            query += " AND m.timestamp >= ?"
            params.append(start_ts)
        
        if end_ts:
            query += " AND m.timestamp <= ?"
            params.append(end_ts)
            
        query += " ORDER BY m.timestamp DESC"
        
        with self.conn:
            cursor = self.conn.execute(query, params)
            profiles = []
            for row in cursor.fetchall():
                profile = {
                    'name': row[0],
                    'url': row[1],
                    'channel_name': row[2],
                    'datetime': row[3],
                    'message_text': row[4]
                }
                profiles.append(profile)
            
            return profiles
    
    def get_user_by_id(self, user_id: str) -> Optional[Dict]:
        """Get user information by ID."""
        with self.conn:
            cursor = self.conn.execute(
                "SELECT id, name, username, email FROM users WHERE id = ?",
                (user_id,)
            )
            row = cursor.fetchone()
            if row:
                return {
                    'id': row[0],
                    'name': row[1],
                    'username': row[2],
                    'email': row[3]
                }
            return None
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()

class InteractiveSlackAnalyzer(cmd.Cmd):
    """Interactive command-line interface for Slack data analysis."""
    
    intro = """
Welcome to the Interactive Slack Data Analyzer!
Type 'help' or '?' to list commands.
First, use 'sync' to download your Slack data.
    """
    prompt = '🔍 slack-analyzer> '
    
    def __init__(self):
        """Initialize the analyzer with necessary components."""
        super().__init__()
        self.token = os.getenv("SLACK_USER_TOKEN")
        self.anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
        
        if not self.token:
            print("Error: SLACK_USER_TOKEN environment variable not set")
            sys.exit(1)
            
        if not self.anthropic_api_key:
            print("Error: ANTHROPIC_API_KEY environment variable not set")
            sys.exit(1)
        
        self.client = WebClient(token=self.token)
        self.claude = anthropic.Anthropic(api_key=self.anthropic_api_key)
        self.timezone = pytz.timezone("America/Chicago")
        self.data_store = SlackDataStore()
        self.console = Console()
        self.user_email = None
        self.user_info = None
    
    def do_sync(self, arg):
        """Sync Slack data to local database: sync [--days N] [--export]"""
        args = arg.split()
        days = 30  # Default to 30 days
        use_export = False
        
        # Parse arguments
        i = 0
        while i < len(args):
            if args[i] == '--days' and i + 1 < len(args):
                try:
                    days = int(args[i + 1])
                except ValueError:
                    self.console.print("[red]Invalid number of days. Using default (30).[/red]")
                i += 2
            elif args[i] == '--export':
                use_export = True
                i += 1
            else:
                i += 1
        
        if not self.user_email:
            self.user_email = input("Enter your Slack user email: ")
            self.user_info = self.get_user_info(self.user_email)
            if not self.user_info:
                self.console.print("[red]Failed to find user. Check your email and try again.[/red]")
                self.user_email = None
                return
        
        if use_export:
            self.sync_with_export()
        else:
            self.sync_with_api(days)
    
    def sync_with_export(self):
        """Sync data using Slack's Export API."""
        try:
            # Check if we have admin privileges
            auth_test = self.client.auth_test()
            if not auth_test.get('is_admin', False):
                self.console.print("[red]Export API requires admin privileges. Falling back to regular sync.[/red]")
                self.sync_with_api(30)
                return
            
            self.console.print("[yellow]Starting workspace export...[/yellow]")
            
            # Request export
            export_response = self.client.admin_conversations_export()
            export_id = export_response['id']
            
            # Poll for export status
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                transient=True,
            ) as progress:
                task = progress.add_task("Waiting for export to complete...", total=None)
                
                while True:
                    status = self.client.admin_conversations_export_status(export_id=export_id)
                    if status['status'] == 'completed':
                        break
                    elif status['status'] == 'failed':
                        raise Exception(f"Export failed: {status.get('error', 'Unknown error')}")
                    
                    time.sleep(5)  # Poll every 5 seconds
            
            # Download and process the export
            self.console.print("[green]Export complete! Processing data...[/green]")
            
            # Get the export file
            export_file = self.client.admin_conversations_export_download(export_id=export_id)
            
            # Process the export file
            self.process_export_file(export_file)
            
            self.console.print("[green]Successfully synced workspace data![/green]")
            
        except SlackApiError as e:
            if e.response["error"] == "not_allowed_token_type":
                self.console.print("[red]Your token doesn't have permission to use the Export API. Falling back to regular sync.[/red]")
                self.sync_with_api(30)
            else:
                self.console.print(f"[red]Error during export: {e.response['error']}[/red]")
        except Exception as e:
            self.console.print(f"[red]Error during export: {str(e)}[/red]")
    
    def sync_with_api(self, days: int):
        """Sync data using regular Slack API calls."""
        # Calculate date range
        end_date = datetime.now(self.timezone)
        start_date = end_date - timedelta(days=days)
        start_ts = start_date.timestamp()
        end_ts = end_date.timestamp()
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            # Fetch and store users
            task = progress.add_task("Fetching users...", total=None)
            users = self.fetch_all_users()
            self.data_store.store_users(users)
            progress.update(task, description="Users synced!", completed=True)
            
            # Fetch and store channels
            task = progress.add_task("Fetching channels...", total=None)
            channels = self.fetch_all_channels()
            self.data_store.store_channels(channels)
            progress.update(task, description="Channels synced!", completed=True)
            
            # Only process channels with 'candidate-labs' or 'candidatelabs' in the name, and skip 'internal' channels
            active_channels = [
                c for c in channels
                if c['is_member'] and not c['is_archived']
                and 'internal' not in c['name'].lower()
                and (
                    'candidate-labs' in c['name'].lower() or
                    'candidatelabs' in c['name'].lower()
                )
            ]
            task = progress.add_task(f"Processing {len(active_channels)} channels...", total=len(active_channels))
            
            for channel in active_channels:
                progress.update(task, description=f"Processing channel: {channel['name']}")
                self.process_channel(channel, start_ts, end_ts)
                progress.update(task, advance=1)
        
        self.console.print(f"[green]Successfully synced {days} days of Slack data![/green]")
    
    def process_export_file(self, export_file):
        """Process the exported Slack data file."""
        # The export file is a ZIP containing JSON files
        import zipfile
        import json
        from io import BytesIO
        
        with zipfile.ZipFile(BytesIO(export_file)) as zip_ref:
            # Process users
            if 'users.json' in zip_ref.namelist():
                with zip_ref.open('users.json') as f:
                    users = json.load(f)
                    self.data_store.store_users({u['id']: u for u in users})
            
            # Process channels
            if 'channels.json' in zip_ref.namelist():
                with zip_ref.open('channels.json') as f:
                    channels = json.load(f)
                    self.data_store.store_channels(channels)
            
            # Process messages
            for filename in zip_ref.namelist():
                if filename.startswith('channels/') and filename.endswith('.json'):
                    channel_name = filename.split('/')[1].replace('.json', '')
                    with zip_ref.open(filename) as f:
                        messages = json.load(f)
                        channel_id = next((c['id'] for c in channels if c['name'] == channel_name), None)
                        if channel_id:
                            self.data_store.store_messages(messages, channel_id, channel_name)
    
    def do_channels(self, arg):
        """List available channels: channels"""
        channels = self.data_store.get_channels(active_only=True)
        
        table = Table(title="Active Slack Channels")
        table.add_column("Name", style="cyan")
        table.add_column("ID", style="dim")
        
        for channel in sorted(channels, key=lambda x: x['name']):
            table.add_row(channel['name'], channel['id'])
        
        self.console.print(table)
    
    def do_search(self, arg):
        """Search messages: search <query> [--channel <channel_name>] [--days <N>]"""
        if not arg:
            self.console.print("[yellow]Please provide a search query.[/yellow]")
            return
        
        args = arg.split()
        query = args[0]
        channel_name = None
        days = 30
        
        # Parse arguments
        i = 1
        while i < len(args):
            if args[i] == '--channel' and i + 1 < len(args):
                channel_name = args[i + 1]
                i += 2
            elif args[i] == '--days' and i + 1 < len(args):
                try:
                    days = int(args[i + 1])
                except ValueError:
                    self.console.print("[yellow]Invalid number of days. Using default (30).[/yellow]")
                i += 2
            else:
                i += 1
        
        # Calculate date range
        end_date = datetime.now(self.timezone)
        start_date = end_date - timedelta(days=days)
        start_ts = start_date.timestamp()
        end_ts = end_date.timestamp()
        
        # Get channel ID if specified
        channel_id = None
        if channel_name:
            channel = self.data_store.get_channel_by_name(channel_name)
            if not channel:
                self.console.print(f"[red]Channel '{channel_name}' not found.[/red]")
                return
            channel_id = channel['id']
        
        # Search messages
        messages = self.data_store.search_messages(query, channel_id, start_ts, end_ts)
        
        if not messages:
            self.console.print("[yellow]No messages found matching the search criteria.[/yellow]")
            return
        
        table = Table(title=f"Search Results for '{query}'")
        table.add_column("Channel", style="cyan")
        table.add_column("User", style="green")
        table.add_column("Date", style="blue")
        table.add_column("Message", style="white")
        
        for msg in messages:
            # Truncate message text for display
            text = msg['text']
            if len(text) > 100:
                text = text[:97] + "..."
            
            table.add_row(msg['channel_name'], msg['user_name'], msg['datetime'], text)
        
        self.console.print(table)
    
    def do_profiles(self, arg):
        """List LinkedIn profiles mentioned: profiles [--channel <channel_name>] [--days <N>]"""
        args = arg.split()
        channel_name = None
        days = 30
        
        # Parse arguments
        i = 0
        while i < len(args):
            if args[i] == '--channel' and i + 1 < len(args):
                channel_name = args[i + 1]
                i += 2
            elif args[i] == '--days' and i + 1 < len(args):
                try:
                    days = int(args[i + 1])
                except ValueError:
                    self.console.print("[yellow]Invalid number of days. Using default (30).[/yellow]")
                i += 2
            else:
                i += 1
        
        # Calculate date range
        end_date = datetime.now(self.timezone)
        start_date = end_date - timedelta(days=days)
        start_ts = start_date.timestamp()
        end_ts = end_date.timestamp()
        
        # Get channel ID if specified
        channel_id = None
        if channel_name:
            channel = self.data_store.get_channel_by_name(channel_name)
            if not channel:
                self.console.print(f"[red]Channel '{channel_name}' not found.[/red]")
                return
            channel_id = channel['id']
        
        # Get LinkedIn profiles
        profiles = self.data_store.get_linkedin_profiles(channel_id, start_ts, end_ts)
        
        if not profiles:
            self.console.print("[yellow]No LinkedIn profiles found.[/yellow]")
            return
        
        table = Table(title="LinkedIn Profiles")
        table.add_column("Name", style="cyan")
        table.add_column("URL", style="blue")
        table.add_column("Channel", style="green")
        table.add_column("Date", style="dim")
        
        for profile in profiles:
            table.add_row(profile['name'], profile['url'], profile['channel_name'], profile['datetime'])
        
        self.console.print(table)
    
    def do_analyze(self, arg):
        """Analyze channel data with Claude: analyze <channel_name> [--days <N>] [--prompt "custom prompt"]"""
        args = arg.split()
        if not args:
            self.console.print("[yellow]Please specify a channel name.[/yellow]")
            return
        
        channel_name = args[0]
        days = 30
        custom_prompt = None
        
        # Parse arguments
        i = 1
        while i < len(args):
            if args[i] == '--days' and i + 1 < len(args):
                try:
                    days = int(args[i + 1])
                except ValueError:
                    self.console.print("[yellow]Invalid number of days. Using default (30).[/yellow]")
                i += 2
            elif args[i] == '--prompt' and i + 1 < len(args):
                # Extract prompt (may contain spaces)
                if args[i + 1].startswith('"'):
                    prompt_parts = []
                    j = i + 1
                    while j < len(args) and not args[j].endswith('"'):
                        prompt_parts.append(args[j])
                        j += 1
                    if j < len(args):
                        prompt_parts.append(args[j])
                        custom_prompt = ' '.join(prompt_parts).strip('"')
                        i = j + 1
                    else:
                        self.console.print("[red]Unclosed quote in prompt.[/red]")
                        return
                else:
                    custom_prompt = args[i + 1]
                    i += 2
            else:
                i += 1
        
        # Get channel
        channel = self.data_store.get_channel_by_name(channel_name)
        if not channel:
            self.console.print(f"[red]Channel '{channel_name}' not found.[/red]")
            return
        
        # Calculate date range
        end_date = datetime.now(self.timezone)
        start_date = end_date - timedelta(days=days)
        start_ts = start_date.timestamp()
        end_ts = end_date.timestamp()
        
        # Get messages
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            task = progress.add_task(f"Fetching messages from #{channel_name}...", total=None)
            messages = self.data_store.get_messages_by_date_range(start_ts, end_ts, channel['id'])
            progress.update(task, description="Preparing data for analysis...", completed=True)
            
            if not messages:
                self.console.print("[yellow]No messages found in this channel for the specified time period.[/yellow]")
                return
            
            # Prepare data for Claude
            messages_text = []
            linkedin_profiles = []
            
            for msg in messages:
                # Format the main message
                message_text = f"{msg['user_name']} ({msg['datetime']}): {msg.get('text', '')}"
                messages_text.append(message_text)
                
                # Collect LinkedIn profiles
                if msg.get('has_linkedin_url') and 'linkedin_profiles' in msg:
                    linkedin_profiles.extend(msg['linkedin_profiles'])
                
                # Add thread messages if any
                if msg.get('is_thread_parent') and msg.get('thread_messages'):
                    for thread_msg in msg['thread_messages']:
                        thread_text = f"    └─ {thread_msg['user_name']} ({thread_msg['datetime']}): {thread_msg.get('text', '')}"
                        messages_text.append(thread_text)
            
            # Add LinkedIn profiles information if any
            linkedin_info = ""
            if linkedin_profiles:
                linkedin_info = "LinkedIn profiles mentioned:\n" + "\n".join([
                    f"- {profile['name']}: {profile['url']}"
                    for profile in linkedin_profiles
                ])
            
            # Use custom prompt if provided
            if custom_prompt:
                prompt = custom_prompt.format(
                    channel_name=channel_name,
                    linkedin_info=linkedin_info,
                    messages_text="\n".join(messages_text)
                )
            else:
                # Default prompt from original code
                prompt = (
                    "Please analyze the Slack channel \"{channel_name}\" and prepare a comprehensive report based on the messages below.\n\n"
                    "{linkedin_info}\n\n"
                    "Channel messages:\n{messages_text}\n\n"
                ).format(
                    channel_name=channel_name,
                    linkedin_info=linkedin_info,
                    messages_text="\n".join(messages_text)
                )
            
            task = progress.add_task("Generating analysis with Claude...", total=None)
            
            try:
                response = self.claude.messages.create(
                    model="claude-3-7-sonnet-20250219",
                    max_tokens=4000,
                    messages=[{
                        "role": "user",
                        "content": prompt
                    }]
                )
                analysis = response.content[0].text
                progress.update(task, description="Analysis complete!", completed=True)
                
                # Display the analysis
                self.console.print(Panel(
                    Markdown(analysis),
                    title=f"Analysis of #{channel_name}",
                    width=120
                ))
                
            except Exception as e:
                progress.update(task, description="Error generating analysis", completed=True)
                self.console.print(f"[red]Error: {str(e)}[/red]")
    
    def do_ask(self, arg):
        """Ask Claude a question about your Slack data: ask <your question>"""
        if not arg:
            self.console.print("[yellow]Please provide a question.[/yellow]")
            return
        
        question = arg.strip()
        
        # Get active channels
        channels = self.data_store.get_channels(active_only=True)
        channel_names = [c['name'] for c in channels]
        
        # Get some context about available data
        end_date = datetime.now(self.timezone)
        start_date = end_date - timedelta(days=30)  # Last 30 days by default
        start_ts = start_date.timestamp()
        end_ts = end_date.timestamp()
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            task = progress.add_task("Preparing context for your question...", total=None)
            
            # Get some sample data to provide context to Claude
            context = {
                "channels": channel_names[:20],  # Limit to 20 channels for context
                "date_range": f"from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            }
            
            # Find channels that might be relevant to the question
            relevant_keywords = question.lower().split()
            relevant_channels = []
            
            for channel in channels:
                if any(keyword in channel['name'].lower() for keyword in relevant_keywords):
                    relevant_channels.append(channel)
            
            # If we found relevant channels, add some sample messages
            sample_messages = []
            if relevant_channels:
                task = progress.add_task("Fetching relevant messages...", total=None)
                for channel in relevant_channels[:3]:  # Limit to 3 most relevant channels
                    messages = self.data_store.get_messages_by_date_range(
                        start_ts, end_ts, channel['id'], limit=10
                    )
                    if messages:
                        sample_text = [f"Channel: #{channel['name']}"]
                        for msg in messages[:5]:  # Limit to 5 messages per channel
                            sample_text.append(f"  {msg['user_name']} ({msg['datetime']}): {msg.get('text', '')}")
                        sample_messages.append("\n".join(sample_text))
                progress.update(task, completed=True)
            
            # Prepare LinkedIn profiles info
            linkedin_profiles = self.data_store.get_linkedin_profiles(
                start_ts=start_ts, end_ts=end_ts, limit=10
            )
            linkedin_info = ""
            if linkedin_profiles:
                linkedin_info = "Sample LinkedIn profiles mentioned:\n" + "\n".join([
                    f"- {profile['name']} (in #{profile['channel_name']} on {profile['datetime']})"
                    for profile in linkedin_profiles[:5]  # Limit to 5 profiles
                ])
            
            # Build the prompt for Claude
            system_prompt = f"""
You are an assistant helping analyze Slack data. You have access to messages from {len(channels)} channels in a Slack workspace {context['date_range']}.

Available channels: {', '.join(context['channels'])}

{linkedin_info if linkedin_info else 'No LinkedIn profiles found in the recent data.'}

The user is asking: "{question}"

Please answer their question based on the following constraints:
1. If you need specific data to answer that hasn't been provided, explain what would need to be queried
2. If the question is about a specific channel, person, or time period not in the sample data, explain how they could use the tool to find that information
3. Focus on being helpful while acknowledging the limitations of the current context

Based on the question, here are some potentially relevant messages:
{chr(10).join(sample_messages) if sample_messages else "No specifically relevant messages were found in the sample data."}
"""
            
            progress.update(task, description="Analyzing your question...", completed=True)
            
            try:
                task = progress.add_task("Generating response with Claude...", total=None)
                response = self.claude.messages.create(
                    model="claude-3-7-sonnet-20250219",
                    max_tokens=2000,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": question}
                    ]
                )
                answer = response.content[0].text
                progress.update(task, description="Response ready!", completed=True)
                
                # Display the answer
                self.console.print(Panel(
                    Markdown(answer),
                    title="Claude's Response",
                    width=120
                ))
                
            except Exception as e:
                progress.update(task, description="Error generating response", completed=True)
                self.console.print(f"[red]Error: {str(e)}[/red]")
    
    def do_export(self, arg):
        """Export channel data to a markdown file: export <channel_name> [--days <N>]"""
        args = arg.split()
        if not args:
            self.console.print("[yellow]Please specify a channel name.[/yellow]")
            return
        
        channel_name = args[0]
        days = 30
        
        # Parse arguments
        i = 1
        while i < len(args):
            if args[i] == '--days' and i + 1 < len(args):
                try:
                    days = int(args[i + 1])
                except ValueError:
                    self.console.print("[yellow]Invalid number of days. Using default (30).[/yellow]")
                i += 2
            else:
                i += 1
        
        # Get channel
        channel = self.data_store.get_channel_by_name(channel_name)
        if not channel:
            self.console.print(f"[red]Channel '{channel_name}' not found.[/red]")
            return
        
        # Calculate date range
        end_date = datetime.now(self.timezone)
        start_date = end_date - timedelta(days=days)
        start_ts = start_date.timestamp()
        end_ts = end_date.timestamp()
        
        # Export file name
        output_file = f"{channel_name}_export_{start_date.strftime('%Y-%m-%d')}_to_{end_date.strftime('%Y-%m-%d')}.md"
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            task = progress.add_task(f"Exporting messages from #{channel_name}...", total=None)
            
            # Get messages
            messages = self.data_store.get_messages_by_date_range(start_ts, end_ts, channel['id'])
            
            if not messages:
                self.console.print("[yellow]No messages found in this channel for the specified time period.[/yellow]")
                return
            
            # Write to file
            with open(output_file, 'w') as f:
                f.write(f"# Export of #{channel_name}\n")
                f.write(f"From {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}\n\n")
                
                for msg in messages:
                    # Write main message
                    f.write(f"## {msg['user_name']} - {msg['datetime']}\n\n")
                    f.write(f"{msg.get('text', '')}\n\n")
                    
                    # Write thread replies if any
                    if msg.get('is_thread_parent') and msg.get('thread_messages'):
                        f.write("### Thread replies:\n\n")
                        for thread_msg in msg['thread_messages']:
                            f.write(f"**{thread_msg['user_name']} - {thread_msg['datetime']}**\n\n")
                            f.write(f"{thread_msg.get('text', '')}\n\n")
                        f.write("---\n\n")
                    else:
                        f.write("---\n\n")
            
            progress.update(task, description="Export complete!", completed=True)
            self.console.print(f"[green]Successfully exported to {output_file}[/green]")
    
    def do_prompt(self, arg):
        """Create custom prompt template: prompt <template_name>"""
        if not arg:
            self.console.print("[yellow]Please specify a template name.[/yellow]")
            return
        
        template_name = arg.strip()
        prompt_file = f"{template_name}_prompt.txt"
        
        self.console.print("[cyan]Enter your custom prompt template. Use placeholders like {channel_name}, {messages_text}, and {linkedin_info}.[/cyan]")
        self.console.print("[cyan]When done, enter a line with just '---' (three dashes)[/cyan]")
        
        lines = []
        while True:
            line = input()
            if line == "---":
                break
            lines.append(line)
        
        prompt_template = "\n".join(lines)
        
        if not prompt_template.strip():
            self.console.print("[yellow]Prompt template is empty. Aborting.[/yellow]")
            return
        
        # Save the template
        with open(prompt_file, 'w') as f:
            f.write(prompt_template)
        
        self.console.print(f"[green]Prompt template saved to {prompt_file}[/green]")
        self.console.print("[cyan]Use it with: analyze <channel> --prompt @" + template_name + "[/cyan]")
    
    def do_custom(self, arg):
        """Execute a custom analysis with any prompt: custom"""
        self.console.print("[cyan]Enter your custom query for Claude:[/cyan]")
        lines = []
        while True:
            line = input()
            if line == "---":
                break
            lines.append(line)
        
        query = "\n".join(lines)
        
        if not query.strip():
            self.console.print("[yellow]Query is empty. Aborting.[/yellow]")
            return
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            task = progress.add_task("Processing with Claude...", total=None)
            
            try:
                response = self.claude.messages.create(
                    model="claude-3-7-sonnet-20250219",
                    max_tokens=4000,
                    messages=[{
                        "role": "user",
                        "content": query
                    }]
                )
                analysis = response.content[0].text
                progress.update(task, description="Analysis complete!", completed=True)
                
                # Display the analysis
                self.console.print(Panel(
                    Markdown(analysis),
                    title="Custom Analysis",
                    width=120
                ))
                
            except Exception as e:
                progress.update(task, description="Error processing query", completed=True)
                self.console.print(f"[red]Error: {str(e)}[/red]")
    
    def get_user_info(self, email: str) -> Dict:
        """Get user information from Slack API."""
        try:
            result = self.client.users_lookupByEmail(email=email)
            user = result["user"]
            
            full_name = user.get("real_name", "")
            initials = "".join(word[0].upper() for word in full_name.split() if word)
            
            return {
                "id": user["id"],
                "email": email,
                "name": full_name,
                "username": user.get("name", ""),
                "initials": initials
            }
        except SlackApiError as e:
            self.console.print(f"[red]Error finding user: {e}[/red]")
            return None
    
    def fetch_all_users(self) -> Dict[str, Dict]:
        """Fetch all users from Slack API."""
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
                self.console.print(f"[red]Error fetching users: {e}[/red]")
                break
                
        return all_users
    
    def fetch_all_channels(self) -> List[Dict]:
        """Fetch all channels from Slack API."""
        all_channels = []
        cursor = None
        
        while True:
            try:
                result = self.client.conversations_list(
                    types="public_channel,private_channel",
                    limit=200,
                    cursor=cursor
                )
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
                self.console.print(f"[red]Error fetching channels: {e}[/red]")
                break
                
        return all_channels
    
    def get_thread_messages(self, channel_id: str, thread_ts: str) -> List[Dict]:
        """Fetch all messages in a thread."""
        thread_messages = []
        cursor = None
        
        while True:
            try:
                # Add a small delay to avoid rate limits
                time.sleep(0.5)
                
                result = self.client.conversations_replies(
                    channel=channel_id,
                    ts=thread_ts,
                    cursor=cursor,
                    limit=200
                )
                thread_messages.extend(result.get('messages', []))
                
                cursor = result.get('response_metadata', {}).get('next_cursor')
                if not cursor:
                    break
                    
            except SlackApiError as e:
                if e.response["error"] == "ratelimited":
                    retry_after = int(e.response.headers.get('Retry-After', 1))
                    logging.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                else:
                    logging.error(f"Error fetching thread messages: {e.response['error']}")
                    break
            except Exception as e:
                logging.error(f"Unexpected error fetching thread messages: {str(e)}")
                break
                
        return thread_messages
    
    def get_conversation_history(self, channel_id: str, start_ts: float, end_ts: float) -> List[Dict]:
        """Get conversation history for a channel within the specified time range."""
        all_messages = []
        cursor = None
        
        while True:
            try:
                # Add a small delay to avoid rate limits
                time.sleep(0.5)
                
                result = self.client.conversations_history(
                    channel=channel_id,
                    oldest=str(start_ts),
                    latest=str(end_ts),
                    limit=200,
                    cursor=cursor
                )
                all_messages.extend(result.get('messages', []))
                
                cursor = result.get('response_metadata', {}).get('next_cursor')
                if not cursor:
                    break
                    
            except SlackApiError as e:
                if e.response["error"] == "ratelimited":
                    retry_after = int(e.response.headers.get('Retry-After', 1))
                    logging.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                else:
                    logging.error(f"Error fetching conversation history: {e.response['error']}")
                    break
            except Exception as e:
                logging.error(f"Unexpected error fetching conversation history: {str(e)}")
                break
                
        return all_messages
    
    def enrich_message(self, message: Dict, channel_id: str) -> Dict:
        """Enrich message with additional context and formatted text."""
        enriched_msg = message.copy()
        
        # Add timestamp in readable format
        if 'ts' in message:
            ts = float(message['ts'])
            dt = datetime.fromtimestamp(ts, self.timezone)
            enriched_msg['datetime'] = dt.strftime('%Y-%m-%d %H:%M:%S')
            
        # Add user information
        if 'user' in message:
            user_id = message['user']
            user_info = self.data_store.get_user_by_id(user_id)
            if user_info:
                enriched_msg['user_name'] = user_info['name']
                enriched_msg['user_username'] = user_info['username']
            else:
                enriched_msg['user_name'] = "Unknown User"
                enriched_msg['user_username'] = "unknown"
        
        # Check if message has thread
        if message.get('thread_ts') and message.get('thread_ts') == message.get('ts'):
            enriched_msg['is_thread_parent'] = True
            thread_messages = self.get_thread_messages(channel_id, message['thread_ts'])
            # Remove the parent message from thread messages (as it's duplicated)
            thread_messages = [m for m in thread_messages if m.get('ts') != message.get('ts')]
            enriched_msg['thread_messages'] = [self.enrich_message(m, channel_id) for m in thread_messages]
            enriched_msg['thread_count'] = len(thread_messages)
        
        # Extract LinkedIn URLs if present
        if 'text' in message:
            linkedin_pattern = r'(?:https?://)?(?:www\.)?linkedin\.com/in/([^>\s|]+)(?:\|([^>]+))?'
            matches = re.finditer(linkedin_pattern, message.get('text', ''))
            linkedin_profiles = []
            
            for match in matches:
                profile = match.group(1)
                name = match.group(2) if match.group(2) else profile
                url = f"https://linkedin.com/in/{profile}"
                linkedin_profiles.append({
                    'name': name,
                    'url': url
                })
            
            if linkedin_profiles:
                enriched_msg['linkedin_profiles'] = linkedin_profiles
                enriched_msg['has_linkedin_url'] = True
            else:
                enriched_msg['has_linkedin_url'] = False
        
        return enriched_msg
    
    def process_channel(self, channel: Dict, start_ts: float, end_ts: float) -> None:
        """Process a single channel and store its messages."""
        if not channel['is_member'] or channel['is_archived']:
            return
            
        try:
            logging.info(f"Processing channel: {channel['name']}")
            
            # Get all messages in the date range
            messages = self.get_conversation_history(channel['id'], start_ts, end_ts)
            
            if not messages:
                logging.info(f"No messages found in channel {channel['name']}")
                return
                
            # Enrich messages with user info and thread info
            enriched_messages = []
            for msg in messages:
                # Skip thread replies as they'll be included with their parent
                if msg.get('thread_ts') and msg.get('thread_ts') != msg.get('ts'):
                    continue
                    
                try:
                    enriched_msg = self.enrich_message(msg, channel['id'])
                    enriched_messages.append(enriched_msg)
                except Exception as e:
                    logging.error(f"Error enriching message in channel {channel['name']}: {str(e)}")
                    continue
            
            # Store messages in database
            if enriched_messages:
                self.data_store.store_messages(enriched_messages, channel['id'], channel['name'])
                logging.info(f"Stored {len(enriched_messages)} messages from channel {channel['name']}")
                
        except SlackApiError as e:
            logging.error(f"Error processing channel {channel['name']}: {e.response['error']}")
        except Exception as e:
            logging.error(f"Unexpected error processing channel {channel['name']}: {str(e)}")
    
    def do_exit(self, arg):
        """Exit the program."""
        self.data_store.close()
        print("Goodbye!")
        return True
    
    # Aliases
    do_quit = do_exit
    do_q = do_exit


def main():
    # Check for required environment variables
    if not os.getenv("SLACK_USER_TOKEN"):
        print("Error: SLACK_USER_TOKEN environment variable not set")
        print("Please set it in your .env file or environment")
        sys.exit(1)
    
    if not os.getenv("ANTHROPIC_API_KEY"):
        print("Error: ANTHROPIC_API_KEY environment variable not set")
        print("Please set it in your .env file or environment")
        sys.exit(1)
    
    # Start the interactive analyzer
    analyzer = InteractiveSlackAnalyzer()
    try:
        auth_test = analyzer.client.auth_test()
        logging.info(f"Slack token valid for user: {auth_test['user']} in team: {auth_test['team']}")
    except SlackApiError as e:
        print(f"Slack token validation failed: {e}")
        sys.exit(1)
    analyzer.cmdloop()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
