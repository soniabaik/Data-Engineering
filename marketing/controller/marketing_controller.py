from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse

from marketing.service.marketing_service_impl import MarketingServiceImpl

interviewRouter = APIRouter()

# 의존성 주입
async def injectMarketingService() -> MarketingServiceImpl:
    return MarketingServiceImpl()

@interviewRouter.post("/marketing/create-virtual-data")
async def generateVirtualMarketingData(
    marketingDataService: MarketingServiceImpl = Depends(injectMarketingService)
):
    try:
        marketingDataService.generateVirtualMarketingData()

        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={"success": True, "message": "가상 마케팅 데이터 생성 성공"}
        )

    except Exception as e:
        print(f"❌ Error in generateMarketingData(): {str(e)}")
        raise HTTPException(status_code=500, detail="서버 내부 오류 발생")
