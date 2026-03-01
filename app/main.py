from fastapi import FastAPI

from app.api.router import api_router

app = FastAPI(
    title="Aegis Query Language",
    description="Secure Semantic SQL Middleware",
    version="0.1.0",
)

app.include_router(api_router, prefix="/api/v1")

@app.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}
