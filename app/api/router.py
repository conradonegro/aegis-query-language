from fastapi import APIRouter

api_router = APIRouter()

@api_router.get("/status")
async def status() -> dict[str, str]:
    return {"module": "api", "status": "operational"}
