import schwabdev
from dotenv import load_dotenv
import os

load_dotenv()  # loads variables from .env into environment

appKey = os.getenv("appKey")
appSecret = os.getenv("appSecret")

client = schwabdev.Client(app_key=appKey, app_secret=appSecret)

streamer = client.stream

streamer.start()

streamer.send(streamer.level_one_equities("AMD", "0,1,2,3,4,5"))

import time
time.sleep(20)