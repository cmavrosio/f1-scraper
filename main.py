import json
import zlib
import base64
import sqlite3
import re
import requests
import time
from datetime import datetime, timedelta

URL = 'https://livetiming.formula1.com/static/2024/Index.json'
# SQLite setup
conn = sqlite3.connect('car_data.db')
cursor = conn.cursor()

# Create table to store car data
cursor.execute('''
    CREATE TABLE IF NOT EXISTS car_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        car_id TEXT,
        channel_0 INTEGER,
        channel_2 INTEGER,
        channel_3 INTEGER,
        channel_4 INTEGER,
        channel_5 INTEGER,
        channel_45 INTEGER,
        session_key INTEGER,
        meeting_key INTEGER
    )
''')
conn.commit()

# Function to fetch index.json
def fetch_index_json(url=URL):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.content.decode('utf-8-sig')
        return json.loads(data)
    except requests.RequestException as e:
        print(f"Failed to fetch data: {e}")
        return None

# Function to parse start time with GMT offset
def parse_start_time_with_offset(start_date, gmt_offset):
    offset_hours, offset_minutes, _ = map(int, gmt_offset.split(':'))
    offset = timedelta(hours=offset_hours, minutes=offset_minutes)
    start_time = datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S')
    return start_time - offset

# Function to find the next session without a path
def get_next_session_without_path():
    data = fetch_index_json(URL)
    now = datetime.utcnow()
    for meeting in data.get("Meetings", []):
        for session in meeting.get("Sessions", []):
            session_start_time = parse_start_time_with_offset(session['StartDate'], session['GmtOffset'])
            if session_start_time > now and 'Path' not in session:
                return session
    return None

def get_latest_session_with_path():
    index_data = fetch_index_json(URL)
    
    if not index_data:
        print("Failed to fetch index data.")
        return None, None, None

    latest_session_with_path = None
    latest_meeting = None
    now = datetime.utcnow()
    for meeting in index_data.get("Meetings", []):
        for session in meeting.get("Sessions", []):
            session_start_time = parse_start_time_with_offset(session['StartDate'], session['GmtOffset'])
            if session_start_time < now and 'Path' in session:
                latest_session_with_path = session
                latest_meeting = meeting

    if latest_session_with_path:
        print(f"Latest session with path found: {latest_session_with_path['Name']} starting at {latest_session_with_path['StartDate']}")
        result = {
            "path": latest_session_with_path['Path'],
            "meeting_key": latest_meeting['Key'],
            "session_key": latest_session_with_path['Key']
        }
        return result
    else:
        print("No session with an available path found.")
        return {}

# Function to monitor and wait for the session path to become available
def monitor_session_path():
    url = 'https://livetiming.formula1.com/static/2024/Index.json'
    while True:
        index_data = fetch_index_json(url)
        if not index_data:
            print("Error fetching index data, retrying in 1 minute...")
            time.sleep(60)
            continue
        
        next_session = None
        next_meeting = None 
        next_start_time = None
        now = datetime.utcnow()
        
        for meeting in index_data.get("Meetings", []):
            for session in meeting.get("Sessions", []):
                session_start_time = parse_start_time_with_offset(session['StartDate'], session['GmtOffset'])
                print(session_start_time, now, session_start_time > now, 'Path' not in session)
                if session_start_time > now and 'Path' not in session:
                    next_session = session
                    next_meeting = meeting
                    next_start_time = session_start_time 
                    break
                elif session_start_time <= now and 'Path' not in session and session_start_time.date() == now.date():
                    # If the session is today and should have started but the path is not yet available
                    next_session = session
                    next_meeting = meeting
                    next_start_time = session_start_time
                    break
            
            if next_session:
                break
        
        if next_session and 'Path' in next_session:
            print(f"Session path found: {next_session['Path']}")
            result = {
                "path": next_session['Path'],
                "meeting_key": next_meeting['Key'],
                "session_key": next_session['Key']
            }
            return result
        else:
            if next_start_time:
                time_to_start = (next_start_time - now).total_seconds()
                if next_start_time.date() == now.date() and next_start_time <= now:
                    # Session should have started but path is not available yet, check every 5 seconds
                    sleep_interval = 5
                elif time_to_start > 3600:
                    sleep_interval = 600
                elif time_to_start > 600:
                    sleep_interval = 120
                else:
                    sleep_interval = 10
                
                print(f"Session {next_session['Name']} starting at {next_start_time}, no path yet. Checking again in {sleep_interval} seconds.")
                time.sleep(sleep_interval)
            else:
                # If no session is found, wait and retry in 1 minute
                print("No upcoming session found. Retrying in 1 minute...")
                time.sleep(60)


# Function to process a data chunk
def process_chunk(chunk, session_key=1, meeting_key=1):
    rows = []
    
    try:
        chunk_str = chunk.decode('utf-8')
        pattern = r'(\d+:\d+:\d+\.\d+)(.*)'
        match = re.match(pattern, chunk_str)
        if not match:
            return rows

        session_time = match.group(1)
        content = match.group(2).strip('\r').strip('"')

        try:
            line_data = json.loads(content.strip('"'))
        except json.JSONDecodeError:
            s = zlib.decompress(base64.b64decode(content), -zlib.MAX_WBITS)
            line_data = json.loads(s.decode('utf-8-sig'))

        for entry in line_data.get('Entries', []):
            timestamp = entry.get('Utc')
            for car_id, car_data in entry.get('Cars', {}).items():
                channels = car_data.get('Channels', {})
                row = {
                    'timestamp': timestamp,
                    'car_id': car_id,
                    'channel_0': channels.get('0'),
                    'channel_2': channels.get('2'),
                    'channel_3': channels.get('3'),
                    'channel_4': channels.get('4'),
                    'channel_5': channels.get('5'),
                    'channel_45': channels.get('45'),
                    'session_key': session_key,
                    'meeting_key': meeting_key
                }
                rows.append(row)
    
    except Exception as e:
        print(f"Error processing chunk: {e}")
    
    print(f"Processed {len(rows)} rows from this chunk.")
    return rows

# Function to insert rows into the database
def insert_rows_into_db(rows):
    for row in rows:
        cursor.execute('''
            INSERT INTO car_data (timestamp, car_id, channel_0, channel_2, channel_3, channel_4, channel_5, channel_45, session_key, meeting_key)
            VALUES (:timestamp, :car_id, :channel_0, :channel_2, :channel_3, :channel_4, :channel_5, :channel_45, :session_key, :meeting_key)
        ''', row)
    conn.commit()

# Main function to start the process
def main():
    session = get_latest_session_with_path()
    session = monitor_session_path()
    # print("sessionnn", session)
    if session.get('path'):
        url = f'https://livetiming.formula1.com/static/{session.get('path')}CarData.z.jsonStream'
        print(url)
        with requests.get(url, stream=True) as response:
            if response.status_code == 200:
                for chunk in response.iter_lines():
                    if chunk:
                        parsed_data = process_chunk(chunk, session.get('session_key'), session.get('meeting_key'))
                        if parsed_data:
                            insert_rows_into_db(parsed_data)

if __name__ == "__main__":
    main()