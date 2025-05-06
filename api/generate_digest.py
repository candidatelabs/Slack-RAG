#!/usr/bin/env python3

import os
import sys
from datetime import datetime, timedelta
import pytz
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import pandas as pd
import re
from typing import Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

class SlackDigestGenerator:
    def __init__(self, token: str, user_email: str, timezone_str: str = "America/Chicago"):
        """Initialize the digest generator with Slack token and timezone."""
        self.client = WebClient(token=token)
        self.timezone = pytz.timezone(timezone_str)
        self.user_email = user_email
        self.user_identifiers = self.get_user_info()
        self.channels = self.fetch_all_channels()

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
            return None

    def get_date_range(self, start_date_str: str = None, end_date_str: str = None) -> Tuple[float, float]:
        """Get timestamps for the specified date range."""
        if start_date_str:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            start_date = start_date.replace(hour=0, minute=0, second=0)
            start_date = self.timezone.localize(start_date)
        else:
            now = datetime.now(self.timezone)
            start_date = now - timedelta(days=now.weekday() + 7)
            start_date = start_date.replace(hour=0, minute=0, second=0)
            start_date = self.timezone.localize(start_date)
        
        if end_date_str:
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
            end_date = end_date.replace(hour=23, minute=59, second=59)
            end_date = self.timezone.localize(end_date)
        else:
            end_date = start_date + timedelta(days=6)
            end_date = end_date.replace(hour=23, minute=59, second=59)
        
        return start_date.timestamp(), end_date.timestamp()
    
    def process_channel(self, channel, start_ts, end_ts):
        """Process a single channel for messages and candidates"""
        if not channel['is_member'] or channel['is_archived']:
            return []
            
        try:
            result = self.client.conversations_history(
                channel=channel['id'],
                oldest=str(start_ts),
                latest=str(end_ts),
                limit=100
            )
            
            messages = [msg for msg in result.get('messages', []) 
                       if msg.get('user') == self.user_identifiers['id']]
            
            candidates = []
            for msg in messages:
                linkedin_pattern = r'(?:https?://)?(?:www\.)?linkedin\.com/in/([^>\s|]+)(?:\|([^>]+))?'
                matches = re.finditer(linkedin_pattern, msg.get('text', ''))
                
                for match in matches:
                    profile = match.group(1)
                    name = match.group(2) if match.group(2) else profile
                    url = f"https://linkedin.com/in/{profile}"
                    candidates.append({
                        'name': name,
                        'linkedin_url': url,
                        'channel': channel['name'],
                        'timestamp': msg.get('ts', '')
                    })
            
            return candidates
            
        except SlackApiError:
            return []

    def process_messages(self, start_ts: float, end_ts: float) -> pd.DataFrame:
        """Process messages from all relevant channels within the time range."""
        active_channels = [c for c in self.channels 
                         if c['is_member'] and not c['is_archived']]
        
        all_candidates = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_channel = {
                executor.submit(self.process_channel, channel, start_ts, end_ts): channel
                for channel in active_channels
            }
            
            for future in as_completed(future_to_channel):
                channel = future_to_channel[future]
                try:
                    candidates = future.result()
                    all_candidates.extend(candidates)
                except Exception:
                    continue
        
        if all_candidates:
            return pd.DataFrame(all_candidates)
        else:
            return pd.DataFrame(columns=['name', 'linkedin_url', 'channel', 'timestamp'])

def main():
    parser = argparse.ArgumentParser(description='Generate digest of candidate submissions from Slack.')
    parser.add_argument('--start', help='Start date (YYYY-MM-DD)', required=True)
    parser.add_argument('--end', help='End date (YYYY-MM-DD)', required=True)
    parser.add_argument('--user', help='User email', required=True)
    args = parser.parse_args()
    
    token = os.getenv("SLACK_USER_TOKEN")
    if not token:
        print("Error: SLACK_USER_TOKEN environment variable not set")
        sys.exit(1)
    
    generator = SlackDigestGenerator(token, args.user)
    start_ts, end_ts = generator.get_date_range(args.start, args.end)
    
    df = generator.process_messages(start_ts, end_ts)
    if not df.empty:
        output_file = f"my_submissions_{args.start}_to_{args.end}.csv"
        df.to_csv(output_file, index=False)
        print(f"\nSaved {len(df)} submissions to {output_file}")
    else:
        print(f"\nDate range: {args.start} to {args.end}")
        print(f"User: {generator.user_identifiers['name']} ({generator.user_identifiers['email']})")
        print("\nNote: Only messages containing LinkedIn profile URLs are counted as submissions.")
        sys.exit(0)

if __name__ == "__main__":
    main() 