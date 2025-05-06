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

# Load environment variables
load_dotenv()

DEFAULT_PROMPT = (
    "Please act as DK's virtual recruiting assistant. Analyze the Slack channel \"{channel_name}\" and prepare a comprehensive candidate pipeline report based on the messages below.\n\n"
    "Your primary tasks are to:\n"
    "1. Track ALL candidates in the pipeline for this company, regardless of when they were initially submitted\n"
    "2. Distinguish between new submissions and ongoing candidates\n"
    "3. Identify each candidate's current position in the hiring pipeline\n"
    "4. Flag candidates requiring follow-up, especially those with no recent updates\n\n"
    "For each candidate:\n"
    "- Note when they were initially submitted (if mentioned)\n"
    "- Track their current status in the hiring process\n"
    "- Highlight any recent feedback or updates from this reporting period\n"
    "- Flag candidates with no recent activity who require follow-up (over 1 week of no activity, mention)\n\n"
    "Create separate sections for:\n"
    "1. NEW SUBMISSIONS: Candidates newly submitted during this reporting period only\n"
    "2. ACTIVE PIPELINE: ALL candidates in process (including those submitted before this reporting period)\n"
    "   - With updates this week (highlight the new information)\n"
    "   - Without updates this week (note last known status and time since last update)\n"
    "3. FOLLOW-UP NEEDED: Candidates requiring immediate attention (no response, unclear status, etc.)\n"
    "4. ACTION ITEMS: Specific tasks that need attention with deadlines if applicable\n\n"
    "{linkedin_info}\n\n"
    "Channel messages:\n{messages_text}\n\n"
    "Format your response as a structured table with two columns:\n\n"
    "LEFT COLUMN: Company name (\"{channel_name}\")\n\n"
    "RIGHT COLUMN: Pipeline information organized as follows:\n"
    "1. PIPELINE SUMMARY (one-line overview with counts)\n"
    "   - Total candidates in pipeline\n"
    "   - New submissions this reporting period\n"
    "   - Candidates with updates this reporting period\n"
    "   - Candidates needing follow-up\n\n"
    "2. DETAILED SECTIONS (with clear headers):\n"
    "   - NEW SUBMISSIONS: Candidates newly submitted during this reporting period only\n"
    "   - ACTIVE PIPELINE: ALL candidates in process\n"
    "     * With updates this week (highlight the new information)\n"
    "     * Without updates this week (note last known status and time since last update)\n"
    "   - FOLLOW-UP NEEDED: Candidates requiring immediate attention\n"
    "   - ACTION ITEMS: Specific tasks with deadlines if applicable\n\n"
    "Use markdown table formatting for consistency. For example:\n\n"
    "| Company | Pipeline Status |\n"
    "|---------|----------------|\n"
    "| {channel_name} | **PIPELINE SUMMARY**: 12 total candidates \\| 3 new submissions \\| 5 with updates \\| 4 needing follow-up<br><br>**NEW SUBMISSIONS**:<br>• John Smith - Frontend Developer (submitted May 2)<br>• Jane Doe - Product Manager (submitted May 3)<br><br>**ACTIVE PIPELINE**:<br>• Alex Johnson - Interview scheduled May 8<br>• Sarah Williams - Awaiting feedback (2 weeks since last update)<br><br>**FOLLOW-UP NEEDED**:<br>• Michael Brown - No response for 3 weeks<br><br>**ACTION ITEMS**:<br>• Email hiring manager about Michael Brown by EOD |\n\n"
    "This report will help DK quickly understand the current candidate pipeline for each company and prioritize follow-up actions."
)

class SlackDigestGenerator:
    def __init__(self, token: str, user_email: str, timezone_str: str = "America/Chicago", custom_prompt: str = None):
        """Initialize the digest generator with Slack token, timezone, and optional custom prompt."""
        self.client = WebClient(token=token)
        self.timezone = pytz.timezone(timezone_str)
        self.user_email = user_email
        self.user_identifiers = self.get_user_info()
        self.cache_dir = Path('.cache')
        self.cache_dir.mkdir(exist_ok=True)
        self.channel_cache_file = self.cache_dir / 'channel_cache.json'
        self.user_cache_file = self.cache_dir / 'user_cache.json'
        self.channels = self.get_cached_channels()
        self.users = self.get_cached_users()
        self.claude = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        self.custom_prompt = custom_prompt
        
    def get_cached_channels(self):
        """Get channels from cache or fetch if cache doesn't exist/is old"""
        if self.channel_cache_file.exists():
            cache_age = datetime.now().timestamp() - self.channel_cache_file.stat().st_mtime
            if cache_age < 86400:  # Cache is less than 24 hours old
                with open(self.channel_cache_file, 'r') as f:
                    return json.load(f)
        
        channels = self.fetch_all_channels()
        with open(self.channel_cache_file, 'w') as f:
            json.dump(channels, f)
        return channels

    def get_cached_users(self):
        """Get users from cache or fetch if cache doesn't exist/is old"""
        if self.user_cache_file.exists():
            cache_age = datetime.now().timestamp() - self.user_cache_file.stat().st_mtime
            if cache_age < 86400:  # Cache is less than 24 hours old
                with open(self.user_cache_file, 'r') as f:
                    return json.load(f)
        
        users = self.fetch_all_users()
        with open(self.user_cache_file, 'w') as f:
            json.dump(users, f)
        return users

    def fetch_all_users(self):
        """Fetch all users in batches"""
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
                print(f"Error fetching users: {e}")
                break
                
        return all_users

    def fetch_all_channels(self):
        """Fetch all channels in batches"""
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
                print(f"Error fetching channels: {e}")
                break
                
        return all_channels

    def get_user_info(self):
        """Get user information including email, name, and username"""
        try:
            result = self.client.users_lookupByEmail(email=self.user_email)
            user = result["user"]
            
            full_name = user.get("real_name", "")
            initials = "".join(word[0].upper() for word in full_name.split() if word)
            
            return {
                "id": user["id"],
                "email": self.user_email,
                "name": full_name,
                "username": user.get("name", ""),
                "initials": initials
            }
        except SlackApiError as e:
            print(f"Error finding user: {e}")
            sys.exit(1)

    def get_date_range(self, start_date_str: str = None, end_date_str: str = None) -> Tuple[float, float]:
        """
        Get timestamps for the specified date range.
        
        Args:
            start_date_str: String in format 'YYYY-MM-DD' specifying start date
            end_date_str: String in format 'YYYY-MM-DD' specifying end date (inclusive)
            
        Returns:
            Tuple of (start_timestamp, end_timestamp)
        """
        if start_date_str:
            # Parse the start date in the local timezone
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            start_date = start_date.replace(hour=0, minute=0, second=0)
            start_date = self.timezone.localize(start_date)
        else:
            # Default to start of previous week
            now = datetime.now(self.timezone)
            start_date = now - timedelta(days=now.weekday() + 7)
            start_date = start_date.replace(hour=0, minute=0, second=0)
            start_date = self.timezone.localize(start_date)
        
        if end_date_str:
            # Parse the end date in the local timezone and set to end of day
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
            end_date = end_date.replace(hour=23, minute=59, second=59)
            end_date = self.timezone.localize(end_date)
        else:
            # Default to end of the week from start_date
            end_date = start_date + timedelta(days=6)
            end_date = end_date.replace(hour=23, minute=59, second=59)
        
        return start_date.timestamp(), end_date.timestamp()
    
    def get_thread_messages(self, channel_id: str, thread_ts: str) -> List[Dict]:
        """Fetch all messages in a thread."""
        thread_messages = []
        cursor = None
        
        while True:
            try:
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
                print(f"Error fetching thread messages: {e}")
                break
                
        return thread_messages

    def get_conversation_history(self, channel_id: str, start_ts: float, end_ts: float) -> List[Dict]:
        """Get conversation history for a channel within the specified time range."""
        all_messages = []
        cursor = None
        
        while True:
            try:
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
                print(f"Error fetching conversation history: {e}")
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
            if user_id in self.users:
                enriched_msg['user_name'] = self.users[user_id]['name']
                enriched_msg['user_username'] = self.users[user_id]['username']
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

    def process_channel(self, channel: Dict, start_ts: float, end_ts: float) -> Dict:
        """Process a single channel for activity summary."""
        channel_summary = {
            'id': channel['id'],
            'name': channel['name'],
            'messages': [],
            'linkedin_profiles': [],
            'has_activity': False
        }
        
        if not channel['is_member'] or channel['is_archived']:
            return channel_summary
            
        try:
            # Get all messages in the date range
            messages = self.get_conversation_history(channel['id'], start_ts, end_ts)
            
            if not messages:
                return channel_summary
                
            # Enrich messages with user info and thread info
            enriched_messages = []
            for msg in messages:
                # Skip thread replies as they'll be included with their parent
                if msg.get('thread_ts') and msg.get('thread_ts') != msg.get('ts'):
                    continue
                    
                enriched_msg = self.enrich_message(msg, channel['id'])
                enriched_messages.append(enriched_msg)
            
            # Only consider channels with actual messages
            if enriched_messages:
                channel_summary['messages'] = enriched_messages
                channel_summary['has_activity'] = True
                
                # Extract LinkedIn profiles for easier reference
                for msg in enriched_messages:
                    if msg.get('has_linkedin_url'):
                        channel_summary['linkedin_profiles'].extend(msg.get('linkedin_profiles', []))
                
            return channel_summary
            
        except SlackApiError as e:
            print(f"Error processing channel {channel['name']}: {e}")
            return channel_summary

    def generate_channel_summary(self, channel_data: Dict) -> str:
        """Generate a summary of channel activity using Claude."""
        if not channel_data['has_activity']:
            return "No activity in this channel for the specified time period."

        try:
            # Prepare a structured representation of the channel activity
            messages_text = []
            
            for msg in channel_data['messages']:
                # Format the main message
                message_text = f"{msg['user_name']} ({msg['datetime']}): {msg.get('text', '')}"
                messages_text.append(message_text)
                
                # Add thread messages if any
                if msg.get('is_thread_parent') and msg.get('thread_messages'):
                    for thread_msg in msg['thread_messages']:
                        thread_text = f"    └─ {thread_msg['user_name']} ({thread_msg['datetime']}): {thread_msg.get('text', '')}"
                        messages_text.append(thread_text)
            
            # Add LinkedIn profiles information if any
            linkedin_info = ""
            if channel_data['linkedin_profiles']:
                linkedin_info = "LinkedIn profiles mentioned:\n" + "\n".join([
                    f"- {profile['name']}: {profile['url']}"
                    for profile in channel_data['linkedin_profiles']
                ])
            
            # Use custom prompt if provided, else default
            prompt_template = self.custom_prompt if self.custom_prompt else DEFAULT_PROMPT
            prompt = prompt_template.format(
                channel_name=channel_data['name'],
                linkedin_info=linkedin_info,
                messages_text="\n".join(messages_text)
            )

            # Get Claude's analysis
            response = self.claude.messages.create(
                model="claude-3-7-sonnet-20250219",
                max_tokens=2000,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )
            
            return response.content[0].text
            
        except Exception as e:
            print(f"Error generating channel summary: {e}")
            return f"Error generating summary: {str(e)}"

    def process_channels(self, start_ts: float, end_ts: float) -> Dict[str, Any]:
        """Process all channels and generate activity summaries."""
        active_channels = [c for c in self.channels 
                         if c['is_member'] and not c['is_archived']]
        
        print(f"\nProcessing {len(active_channels)} active channels...")
        
        channel_summaries = {}
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_channel = {
                executor.submit(self.process_channel, channel, start_ts, end_ts): channel
                for channel in active_channels
            }
            
            for future in as_completed(future_to_channel):
                channel = future_to_channel[future]
                try:
                    channel_data = future.result()
                    if channel_data['has_activity']:
                        print(f"Found activity in channel: {channel['name']}")
                        channel_summaries[channel['name']] = channel_data
                except Exception as e:
                    print(f"Error processing {channel['name']}: {e}")
        
        return channel_summaries

    def generate_digest(self, start_ts: float, end_ts: float) -> Dict[str, str]:
        """Generate a digest of all channel activities."""
        # Get all channel data
        channel_summaries = self.process_channels(start_ts, end_ts)
        
        if not channel_summaries:
            print("\nNo activity found in any channels for the specified date range.")
            return {}
        
        # Generate summaries for each channel with activity
        digest = {}
        print("\nGenerating summaries with Claude...")
        
        for channel_name, channel_data in channel_summaries.items():
            print(f"Generating summary for {channel_name}...")
            summary = self.generate_channel_summary(channel_data)
            digest[channel_name] = summary
        
        return digest

def main():
    parser = argparse.ArgumentParser(description='Generate digest of channel activities from Slack.')
    parser.add_argument('--start', help='Start date (YYYY-MM-DD)', required=True)
    parser.add_argument('--end', help='End date (YYYY-MM-DD)', required=True)
    parser.add_argument('--user', help='User email', required=True)
    parser.add_argument('--timezone', help='Timezone (e.g., America/Chicago)', default="America/Chicago")
    args = parser.parse_args()
    
    token = os.getenv("SLACK_USER_TOKEN")
    if not token:
        print("Error: SLACK_USER_TOKEN environment variable not set")
        sys.exit(1)
    
    if not os.getenv("ANTHROPIC_API_KEY"):
        print("Error: ANTHROPIC_API_KEY environment variable not set")
        sys.exit(1)
    
    print(f"\nGenerating Slack digest from {args.start} to {args.end}")
    print(f"Using timezone: {args.timezone}")
    
    generator = SlackDigestGenerator(token, args.user, args.timezone)
    start_ts, end_ts = generator.get_date_range(args.start, args.end)
    
    # Get date in readable format for the file name
    start_date = datetime.fromtimestamp(start_ts, generator.timezone).strftime('%Y-%m-%d')
    end_date = datetime.fromtimestamp(end_ts, generator.timezone).strftime('%Y-%m-%d')
    
    digest = generator.generate_digest(start_ts, end_ts)
    if digest:
        # Create output file
        output_file = f"client_digest_{start_date}_to_{end_date}.md"
        with open(output_file, 'w') as f:
            f.write(f"# Client Activity Digest ({start_date} to {end_date})\n\n")
            f.write(f"Generated for: {generator.user_identifiers['name']} ({generator.user_identifiers['email']})\n\n")
            
            for channel_name, summary in digest.items():
                f.write(f"## {channel_name}\n\n")
                f.write(f"{summary}\n\n")
                f.write("---\n\n")
        
        print(f"\nSaved client digest to {output_file}")
    else:
        print(f"\nNo activity found for date range: {args.start} to {args.end}")
        sys.exit(0)

if __name__ == "__main__":
    main() 