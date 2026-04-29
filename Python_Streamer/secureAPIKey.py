import asyncio


class SecureAPIKey:
    def __init__(self, initial_key: str):
        self._key = initial_key
        # Switch to asyncio Lock
        self._lock = asyncio.Lock()
        self.rate_limited = False

    async def lock_key(self):
        # Must use await here!
        await self._lock.acquire()

    def unlock_key(self):
        if self._lock.locked():
            self._lock.release()

    def rate_limit(self):
        self.rate_limited = True

    @property
    def key(self):
        # NOTE: Since you manually lock/unlock, we just return the string.
        # But be careful: calling this without holding the lock is risky.
        return self._key
