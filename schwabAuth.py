import base64
import json
from pathlib import Path
import time
from types import SimpleNamespace
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv
import os

load_dotenv()  # loads variables from .env into environment

appKey = os.getenv("appKey")
appSecret = os.getenv("secretKey")

access_token = None
refresh_token = None
code = None

authUrl = f" https://api.schwabapi.com/v1/oauth/authorize?client_id={appKey}&redirect_uri=https://127.0.0.1"


file_path = Path("tokens.json")
if file_path.exists():
    with open(file_path, "r", encoding="utf-8") as file:
        parsed_json = json.load(file, object_hook=lambda d: SimpleNamespace(**d))
        if hasattr(parsed_json, 'token_dictionary'):
            code = parsed_json.code
            access_token = parsed_json.access_token
            refresh_token = parsed_json.refresh_token
            old_timestamp = parsed_json.timestamp  

            # Convert it to a datetime object
            old_time = datetime.fromtimestamp(old_timestamp)

            # Get the current time
            now = datetime.now() 
            if now - old_time > timedelta(minutes=28):
                with open("tokens.json", "w", encoding="utf-8") as file:
                    file.truncate(0)

                headers = {'Authorization': f'Basic {base64.b64encode(bytes(f"{appKey}:{appSecret}", "utf-8")).decode("utf-8")}',
                                'Content-Type': 'application/x-www-form-urlencoded'}
                data = {'grant_type': 'authorization_code',
                                    'code': code,
                                    'redirect_uri': "https://127.0.0.1"}

                response = requests.post('https://api.schwabapi.com/v1/oauth/token', headers=headers, data=data)

                tD = response.json()

                access_token = tD['access_token']
                refresh_token = tD['refresh_token']

                timestamp = datetime.now().timestamp()

                tokens_data = {
                    "code": code,
                    "access_token": access_token,
                    "refresh_token": refresh_token,
                    "timestamp": timestamp
                }
                with open(file_path, "w", encoding="utf-8") as file:
                    json.dump(tokens_data, file, indent=4)  # indent=4 makes it pretty-printed
else: 
    print(f"Click to authenticate: {authUrl}")

    returnedLink = input("Paste the redirect URL here:")

    code = f"{returnedLink[returnedLink.index('code=')+5:returnedLink.index('%40')]}@"

    headers = {'Authorization': f'Basic {base64.b64encode(bytes(f"{appKey}:{appSecret}", "utf-8")).decode("utf-8")}',
                    'Content-Type': 'application/x-www-form-urlencoded'}
    data = {'grant_type': 'authorization_code',
                        'code': code,
                        'redirect_uri': "https://127.0.0.1"}

    response = requests.post('https://api.schwabapi.com/v1/oauth/token', headers=headers, data=data)

    tD = response.json()

    access_token = tD['access_token']
    refresh_token = tD['refresh_token']

    timestamp = datetime.now().timestamp()

    tokens_data = {
        "code": code,
        "access_token": access_token,
        "refresh_token": refresh_token,
        "timestamp": timestamp
    }
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(tokens_data, file, indent=4)  # indent=4 makes it pretty-printed

