from typing import List
from marketing.entity.marketing_data import MarketingData
from marketing.repository.marketing_repository import MarketingRepository


class MarketingRepositoryImpl(MarketingRepository):

    def bulkCreate(self, data: List[MarketingData]) -> None:
        for item in data:
            print(item.model_dump_json())

        print(f"✅ {len(data)}개의 마케팅 데이터 저장 완료")
