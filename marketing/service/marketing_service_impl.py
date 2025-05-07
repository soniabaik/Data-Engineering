from random import choices, randint

from marketing.entity.marketing_data import MarketingData
from marketing.entity.campaign_type import CampaignType
from marketing.entity.gender import Gender
from marketing.entity.user_response_type import UserResponseType
from marketing.repository.marketing_repository_impl import MarketingRepositoryImpl
from marketing.service.marketing_service import MarketingService


class MarketingServiceImpl(MarketingService):
    def __init__(self):
        self.marketingRepository = MarketingRepositoryImpl()

    def __generateSingle(self) -> MarketingData:
        # 성별: 여성 60%, 남성 40%
        gender = choices(
            population=[Gender.female, Gender.male],
            weights=[0.6, 0.4],
            k=1
        )[0]

        # 연령: 20대 40%, 30대 40%, 그 외 20%
        age_group = choices(
            population=["20s", "30s", "other"],
            weights=[0.4, 0.4, 0.2],
            k=1
        )[0]

        if age_group == "20s":
            age = randint(20, 29)
        elif age_group == "30s":
            age = randint(30, 39)
        else:
            age = randint(18, 65)

        # 캠페인 타입: Email 고정
        campaign_type = CampaignType.EMAIL

        # 유저 응답: 무시 10%, 클릭 70%, 구매 20%
        user_response = choices(
            population=[
                UserResponseType.ignored,
                UserResponseType.clicked,
                UserResponseType.purchased
            ],
            weights=[0.1, 0.7, 0.2],
            k=1
        )[0]

        return MarketingData(
            customer_id=randint(1000, 9999),
            age=age,
            gender=gender,
            campaign_id=str(randint(100000, 999999)),
            campaign_type=campaign_type,
            user_response=user_response
        )

    def generateVirtualMarketingData(self):
        virtual_data_list = [self.__generateSingle() for _ in range(100)]
        self.marketingRepository.bulkCreate(virtual_data_list)
        return {"status": "success", "count": len(virtual_data_list)}

