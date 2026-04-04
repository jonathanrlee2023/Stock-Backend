class AppState:
    def __init__(self):
        self.engines = {}
        self.price_db = None
        self.earnings_db = None
        self.tracker_db = None
        self.last_checked_db = None
        self.cpu_executer = None
        self.client = None
        self.streamer = None
        self.httpx_client = None

# Instantiate it once
app_state = AppState()