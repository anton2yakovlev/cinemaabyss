import os
from fastapi import FastAPI

app = FastAPI(title="CinemaAbyss Events Service (Заглушка)")

PORT = int(os.getenv("PORT", 8082))


@app.get("/health")
async def health_check():
    """Хэлс чек events-сервиса"""
    return {
        "status": "healthy",
        "service": "events-service",
        "note": "Заглушка - сервис не реализован"
    }


@app.api_route("/api/events/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def events_stub(path: str):
    """Заглушка для всех events эндпоинтов"""
    return {
        "status": "stub",
        "message": "Events service не реализован",
        "path": f"/api/events/{path}"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)

