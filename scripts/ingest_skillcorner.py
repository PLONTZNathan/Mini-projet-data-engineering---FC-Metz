
import os
from dotenv import load_dotenv
from skillcorner.client import SkillcornerClient

username = os.getenv('SKILLCORNER_USERNAME')
password = os.getenv('SKILLCORNER_PASSWORD')

print("Testing SkillCorner connection...")
try:
    client = SkillcornerClient(
        username=username,
        password=password
    )
    
    print(" Connection successful!")
    print(f"   Authentication works with username: {username}")
    
except Exception as e:
    print(f" Connection failed: {e}")