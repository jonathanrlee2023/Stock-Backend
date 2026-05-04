from abc import ABC, abstractmethod

class FinancialDataSource(ABC):
    @abstractmethod
    async def fetch_fundamentals(self, category):
        raise NotImplementedError

    @abstractmethod
    async def get_company_overviews(self):
        raise NotImplementedError

    @abstractmethod
    async def replace_with_usd(self):
        raise NotImplementedError