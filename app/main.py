from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from app.api.routes import router as api_router, warmup_pipelines
from app.api.auth_routes import router as auth_router
from app.core.config import settings
import uvicorn
import os

app = FastAPI(
    title=settings.APP_NAME,
    description="Multi-tenant AI Knowledge Engine with BYO-LLM",
    version="0.1.0"
)

@app.on_event("startup")
async def startup_event():
    """Run startup tasks."""
    # Warmup AI pipelines for active tenants to reduce representation latency on first query
    import asyncio
    asyncio.create_task(warmup_pipelines())

# Include API routers
app.include_router(auth_router, prefix="/api/v1")
app.include_router(api_router, prefix="/api/v1")

# Serve static files (frontend)
frontend_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend")
if os.path.exists(frontend_path):
    app.mount("/static", StaticFiles(directory=frontend_path), name="static")

@app.get("/health")
def health_check():
    return {"status": "ok", "app": settings.APP_NAME}

@app.get("/")
def serve_frontend():
    """Serve the admin dashboard"""
    index_path = os.path.join(frontend_path, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"message": "Frontend not found. Access API at /docs"}

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True, reload_dirs=["app"])
