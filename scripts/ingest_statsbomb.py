
import os
from dotenv import load_dotenv
from statsbombpy import sb

# Load credentials

username = os.getenv('STATSBOMB_USERNAME')
password = os.getenv('STATSBOMB_PASSWORD')

try:
    creds = {
        'user': username,
        'passwd': password
    }
    
    result = sb.competitions(creds=creds)
    
    print(" Connection successful!")
    print(f"  Authentication works with username: {username}")
    
except Exception as e:
    print(f"Connection failed: {e}")