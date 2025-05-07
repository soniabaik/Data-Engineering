from abc import ABC, abstractmethod
from typing import List
from marketing.entity.marketing_data import MarketingData


class MarketingRepository(ABC):

    @abstractmethod
    def bulkCreate(self, data: List[MarketingData]) -> None:
        pass
