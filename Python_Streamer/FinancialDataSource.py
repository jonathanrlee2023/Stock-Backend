from abc import ABC, abstractmethod

class FinancialDataSource(ABC):
    @abstractmethod
    async def fetch_fundamentals(self, ticker):
        pass