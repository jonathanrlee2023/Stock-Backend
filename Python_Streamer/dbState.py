class AppState:
    def __init__(self):
        self.engines = {}
        self.price_db = None
        self.earnings_db = None
        self.tracker_db = None
        # Add any other shared resources here

# Instantiate it once
db_state = AppState()