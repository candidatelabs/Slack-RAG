from flask import Flask, render_template, request, jsonify, send_file
import os
from datetime import datetime
import pytz
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from generate_digest import SlackDigestGenerator
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = Flask(__name__)

def get_slack_users():
    """Fetch all users from Slack"""
    token = os.getenv("SLACK_USER_TOKEN")
    if not token:
        return []
        
    client = WebClient(token=token)
    users = []
    cursor = None
    
    try:
        while True:
            result = client.users_list(limit=200, cursor=cursor)
            for user in result["members"]:
                # Filter out bots, deleted users, and users without email
                if (not user.get("is_bot") and 
                    not user.get("deleted") and 
                    user.get("profile", {}).get("email")):
                    users.append({
                        "id": user["id"],
                        "email": user["profile"]["email"],
                        "name": user["profile"].get("real_name", ""),
                        "title": user["profile"].get("title", "")
                    })
            
            cursor = result.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break
                
    except SlackApiError as e:
        print(f"Error fetching users: {e}")
        
    return sorted(users, key=lambda x: x['name'].lower())

@app.route('/api/users')
def users():
    """API endpoint to get list of users"""
    users = get_slack_users()
    return jsonify(users)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        start_date = request.form['start_date']
        end_date = request.form['end_date']
        user_email = request.form['user_email']
        
        token = os.getenv("SLACK_USER_TOKEN")
        if not token:
            return "Error: SLACK_USER_TOKEN environment variable not set", 400
            
        try:
            # Use the original SlackDigestGenerator
            generator = SlackDigestGenerator(token, user_email)
            start_ts, end_ts = generator.get_date_range(start_date, end_date)
            
            # Get submissions using the original processing logic
            df = generator.process_messages(start_ts, end_ts)
            
            if df is not None and not df.empty:
                # Convert timestamp to datetime for sorting and formatting
                df['timestamp'] = pd.to_datetime(df['timestamp'].astype(float), unit='s')
                df['submission_date'] = df['timestamp'].dt.strftime('%m/%d')
                
                # Filter out internal channels
                df = df[~df['channel'].str.startswith('internal-')]
                
                # Extract client name from channel name
                def extract_client_name(channel_name):
                    parts = channel_name.lower().split('-')
                    
                    # Handle candidatelabs- prefix
                    if 'candidatelabs' in parts:
                        idx = parts.index('candidatelabs')
                        if len(parts) > idx + 1:
                            return parts[idx + 1].capitalize()
                    
                    # Handle clientchannel- prefix
                    if any(p.startswith('clientchannel') for p in parts):
                        for i, part in enumerate(parts):
                            if part.startswith('clientchannel'):
                                if len(parts) > i + 1:
                                    return parts[i + 1].capitalize()
                    
                    # Handle candidate-labs prefix (alternative format)
                    if 'candidate' in parts and 'labs' in parts:
                        idx = parts.index('labs')
                        if len(parts) > idx + 1:
                            return parts[idx + 1].capitalize()
                    
                    # If no pattern matches, return the second part if it exists
                    return parts[1].capitalize() if len(parts) > 1 else parts[0].capitalize()
                
                # Extract client names
                df['client'] = df['channel'].apply(extract_client_name)
                
                # Create client-date pairs for each submission
                df['client_with_date'] = df.apply(
                    lambda row: f"{row['client']} ({row['submission_date']})",
                    axis=1
                )
                
                # Group by candidate and aggregate all their submissions
                output_df = df.groupby(['name', 'linkedin_url']).agg({
                    'client_with_date': lambda x: list(x),  # Keep all submissions as a list
                    'timestamp': 'max'  # Keep most recent timestamp for sorting
                }).reset_index()
                
                # Sort submissions chronologically within each group
                output_df['Clients Submitted To'] = output_df['client_with_date'].apply(
                    lambda x: ', '.join(sorted(x, key=lambda d: d.split('(')[1]))  # Sort by date
                )
                
                # Sort candidates by most recent submission
                output_df = output_df.sort_values('timestamp', ascending=False)
                
                # Rename columns to match expected format
                output_df = output_df.rename(columns={
                    'name': 'Candidate Name',
                    'linkedin_url': 'LinkedIn URL'
                })
                
                # Select final columns
                output_df = output_df[['Candidate Name', 'LinkedIn URL', 'Clients Submitted To']]
                
                # Generate output file
                output_file = f"my_submissions_{start_date}_to_{end_date}.csv"
                output_df.to_csv(output_file, index=False)
                
                return send_file(
                    output_file,
                    mimetype='text/csv',
                    as_attachment=True,
                    download_name=output_file
                )
            else:
                return f"No submissions found for {user_email} between {start_date} and {end_date}"
                
        except Exception as e:
            return f"Error: {str(e)}", 400
            
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True) 