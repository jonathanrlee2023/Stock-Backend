import base64
import json
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime, timedelta
import requests

appKey = "GrxCcHDYFQxZIUhn2vXadnZGKLvG5m4h"
appSecret = "tZwSTZiDQ7lTXhOr"

access_token = None
refresh_token = None

authUrl = f" https://api.schwabapi.com/v1/oauth/authorize?client_id={appKey}&redirect_uri=https://127.0.0.1"


file_path = Path("tokens.json")
if file_path.exists():
    with open(file_path, "r", encoding="utf-8") as file:
        parsed_json = json.load(file, object_hook=lambda d: SimpleNamespace(**d))
        if hasattr(parsed_json, 'access_token') and hasattr(parsed_json, 'refresh_token'):
            access_token = parsed_json.access_token
            refresh_token = parsed_json.refresh_token
            # Assume this is the old timestamp (as a float or int)
            old_timestamp = parsed_json.timestamp  

            # Convert it to a datetime object
            old_time = datetime.fromtimestamp(old_timestamp)

            # Get the current time
            now = datetime.now() 
            if now - old_time > timedelta(minutes=28):
                with open("tokens.json", "w", encoding="utf-8") as file:
                    file.truncate(0)

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
        "access_token": access_token,
        "refresh_token": refresh_token,
        "timestamp": timestamp
    }
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(tokens_data, file, indent=4)  # indent=4 makes it pretty-printed


baseUrl = "https://api.schwabapi.com/trader/v1/"

response = requests.get(f"{baseUrl}accounts/accountNumbers", headers={"Authorization": f'Bearer {access_token}'})

if response.status_code == 200:
    data = response.json()
    print('Data:', data)
else:
    print('Error:', response.status_code, response.text)
