import random
from typing import Optional
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
import httpx
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Настройки приложения из переменных окружения"""
    port: int = 8000
    monolith_url: str = "http://localhost:8080"
    movies_service_url: str = "http://localhost:8081"
    events_service_url: str = "http://localhost:8082"
    gradual_migration: bool = True
    movies_migration_percent: int = 50

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False
    )


settings = Settings()
app = FastAPI(title="CinemaAbyss Proxy Service")
print("migration percent", settings.movies_migration_percent)


async def proxy_request(
    url: str,
    method: str,
    path: str,
    headers: dict,
    body: Optional[bytes] = None,
    params: dict = None
) -> Response:
    """
    Проксирует запрос к целевому сервису
    """
    target_url = f"{url}{path}"
    print("target_url", target_url)
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.request(
                method=method,
                url=target_url,
                headers=headers,
                content=body,
                params=params,
                follow_redirects=False
            )
            
            # Убираем заголовки которые могут конфликтовать
            response_headers = dict(response.headers)
            headers_to_remove = [
                "content-length",
                "transfer-encoding",
                "content-encoding",
                "connection"
            ]
            for header in headers_to_remove:
                response_headers.pop(header, None)
            
            return Response(
                content=response.content,
                status_code=response.status_code,
                headers=response_headers,
                media_type=response.headers.get("content-type")
            )
        except httpx.RequestError as e:
            return JSONResponse(
                status_code=503,
                content={"error": "Service unavailable", "details": str(e)}
            )


def should_route_to_microservice() -> bool:
    """
    Определяет, нужно ли направить запрос к микросервису
    """
    if not settings.gradual_migration:
        return True
    
    return random.randint(1, 100) <= settings.movies_migration_percent


@app.get("/health")
async def health_check():
    """Хэлс чек прокси-сервиса"""
    return {
        "status": "healthy",
        "service": "proxy-service",
        "gradual_migration": settings.gradual_migration,
        "movies_migration_percent": settings.movies_migration_percent
    }


async def handle_movies_proxy(request: Request, path: str = ''):
    """
    Общая логика для проксирования запросов к movies API
    """
    body = await request.body()
    
    # Решаем куда направить запрос
    if should_route_to_microservice():
        print("Направляем запрос к микросервису")
        target_url = settings.movies_service_url
        service = "movies-microservice"
    else:
        print("Направляем запрос к монолиту")
        target_url = settings.monolith_url
        service = "monolith"
    
    print(f"Routing /api/movies/{path} to {service}")
    target_path = f"/api/movies/{path}" if path else "/api/movies"

    
    return await proxy_request(
        url=target_url,
        method=request.method,
        path=target_path,
        headers=dict(request.headers),
        body=body if body else None,
        params=dict(request.query_params)
    )


@app.api_route("/api/movies", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def movies_proxy_root(request: Request):
    """
    Проксирует запросы к /api/movies (без дополнительного path)
    """
    print("Сработал movies_proxy_root")
    return await handle_movies_proxy(request, path='')


@app.api_route("/api/movies/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def movies_proxy(request: Request, path: str):
    """
    Проксирует запросы к movies API с дополнительным path
    В зависимости от настроек направляет к монолиту или микросервису
    """
    print("Сработал movies_proxy")
    return await handle_movies_proxy(request, path=path)


@app.api_route("/api/events/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def events_proxy(path: str, request: Request):
    """
    Проксирует запросы к events API (всегда к микросервису)
    """
    body = await request.body()
    
    print(f"Routing /api/events/{path} to events-microservice")
    
    return await proxy_request(
        url=settings.events_service_url,
        method=request.method,
        path=f"/api/events/{path}",
        headers=dict(request.headers),
        body=body if body else None,
        params=dict(request.query_params)
    )


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def default_proxy(path: str, request: Request):
    """
    Проксирует все остальные запросы к монолиту
    """
    body = await request.body()
    
    print(f"Routing /{path} to monolith")
    print("Сработал default_proxy")
    
    return await proxy_request(
        url=settings.monolith_url,
        method=request.method,
        path=f"/{path}",
        headers=dict(request.headers),
        body=body if body else None,
        params=dict(request.query_params)
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=settings.port)

