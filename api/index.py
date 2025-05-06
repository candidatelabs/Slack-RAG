from flask import Flask, render_template, request, jsonify, Response
import os
from datetime import datetime
import pytz
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import pandas as pd
import io
from generate_digest import SlackDigestGenerator
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
from urllib.parse import parse_qs, urlparse

# No need for dotenv in production (Vercel handles env vars)
if os.getenv('VERCEL_ENV') is None:
    from dotenv import load_dotenv
    load_dotenv()

app = Flask(__name__)

def get_slack_users():
    """Fetch all users from Slack"""
    token = os.environ.get("SLACK_USER_TOKEN")
    if not token:
        return []
        
    client = WebClient(token=token)
    users = []
    cursor = None
    
    try:
        while True:
            result = client.users_list(limit=200, cursor=cursor)
            for user in result["members"]:
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
                
    except SlackApiError:
        pass
        
    return sorted(users, key=lambda x: x['name'].lower())

@app.route('/api/users')
def users():
    """API endpoint to get list of users"""
    users = get_slack_users()
    return jsonify(users)

def process_submissions(start_date, end_date, user_email):
    """Process submissions with the working logic"""
    token = os.environ.get("SLACK_USER_TOKEN")
    if not token:
        raise ValueError("SLACK_USER_TOKEN environment variable not set")
        
    generator = SlackDigestGenerator(token, user_email)
    start_ts, end_ts = generator.get_date_range(start_date, end_date)
    df = generator.process_messages(start_ts, end_ts)
    
    if df.empty:
        return None
        
    # Convert timestamp to datetime for sorting and formatting
    df['timestamp'] = pd.to_datetime(df['timestamp'].astype(float), unit='s')
    df['submission_date'] = df['timestamp'].dt.strftime('%m/%d')
    
    # Filter out internal channels
    df = df[~df['channel'].str.startswith('internal-')]
    
    if df.empty:
        return None
    
    # Create client-date pairs for each submission
    df['client_with_date'] = df.apply(
        lambda row: f"{row['channel']} ({row['submission_date']})",
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
    return output_df[['Candidate Name', 'LinkedIn URL', 'Clients Submitted To']]

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        start_date = request.form['start_date']
        end_date = request.form['end_date']
        user_email = request.form['user_email']
        
        try:
            output_df = process_submissions(start_date, end_date, user_email)
            
            if output_df is None:
                return "No submissions found for the specified date range", 404
                
            # Generate CSV in memory
            output = io.StringIO()
            output_df.to_csv(output, index=False)
            output.seek(0)
            
            # Return CSV as a download
            filename = f"my_submissions_{start_date}_to_{end_date}.csv"
            return Response(
                output.getvalue(),
                mimetype='text/csv',
                headers={
                    'Content-Disposition': f'attachment; filename="{filename}"',
                    'Content-Type': 'text/csv; charset=utf-8'
                }
            )
                
        except ValueError as e:
            return f"Error: {str(e)}", 400
        except Exception as e:
            return f"Error: {str(e)}", 500
            
    return render_template('index.html')

def handle_request(request):
    """Internal request handler"""
    # Parse the URL to get the path
    parsed_url = urlparse(request.url)
    path = parsed_url.path
    
    # Parse query parameters
    query_params = parse_qs(parsed_url.query)
    
    # Handle /api/users endpoint
    if path == '/api/users':
        users = get_slack_users()
        return {
            'statusCode': 200,
            'body': json.dumps(users),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
    
    # Handle main endpoint for submissions
    if path == '/api/submissions':
        start_date = query_params.get('start_date', [None])[0]
        end_date = query_params.get('end_date', [None])[0]
        user_email = os.environ.get('SLACK_USER_EMAIL')
        
        if not user_email:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'SLACK_USER_EMAIL environment variable not set'}),
                'headers': {'Content-Type': 'application/json'}
            }
        
        try:
            df = process_submissions(start_date, end_date, user_email)
            
            if df is None:
                return {
                    'statusCode': 404,
                    'body': json.dumps({'error': 'No submissions found'}),
                    'headers': {'Content-Type': 'application/json'}
                }
            
            # Handle CSV format
            if query_params.get('format', [''])[0] == 'csv':
                output = io.StringIO()
                df.to_csv(output, index=False)
                filename = f"my_submissions_{start_date}_to_{end_date}.csv"
                
                return {
                    'statusCode': 200,
                    'body': output.getvalue(),
                    'headers': {
                        'Content-Type': 'text/csv',
                        'Content-Disposition': f'attachment; filename="{filename}"'
                    }
                }
            
            # Default to JSON response
            return {
                'statusCode': 200,
                'body': json.dumps(df.to_dict('records')),
                'headers': {'Content-Type': 'application/json'}
            }
            
        except Exception as e:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': str(e)}),
                'headers': {'Content-Type': 'application/json'}
            }
    
    # Handle root path
    if path == '/':
        return {
            'statusCode': 200,
            'body': 'Slack Digest API',
            'headers': {'Content-Type': 'text/plain'}
        }
    
    # Handle 404 for unknown paths
    return {
        'statusCode': 404,
        'body': json.dumps({'error': 'Not found'}),
        'headers': {'Content-Type': 'application/json'}
    }

# This is the Vercel serverless function handler
def handler(request):
    try:
        return handle_request(request)
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal server error',
                'details': str(e)
            }),
            'headers': {'Content-Type': 'application/json'}
        }

# For local development
if __name__ == '__main__':
    app.run(debug=True) 