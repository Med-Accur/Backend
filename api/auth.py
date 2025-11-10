from fastapi import APIRouter, Depends, Response, Cookie
from dto.auth_dto import LoginRequest, LoginResponse, MeResponse, LogoutResponse, MultiRpcRequest
from Deps.auth import current_user_from_cookies
from services.auth_service import login_service, logout_service
from services.config_service import me_service, get_widget_data
import redis
import os
import json

router = APIRouter(prefix="/api/auth", tags=["auth"])

# Redis client (synchrone)
redis_client = redis.from_url(
    os.getenv("REDIS_URL"),
    decode_responses=True
    #ssl=True   indispensable pour Redis Cloud (TLS)
)

@router.get("/test_redis")
def test_redis():
    try:
        redis_client.set("ping", "pong", ex=60)
        return {"status": "ok", "value": redis_client.get("ping")}
    except Exception as e:
        return {"status": "error", "details": str(e)}
    
@router.get("/redis-keys")
def list_keys():
    keys = redis_client.keys("*")
    return {"count": len(keys), "keys": keys}

@router.get("/redis-get/{key}")
def get_value(key: str):
    value = redis_client.get(key)
    return {"key": key, "value": value}


@router.post("/login", response_model=LoginResponse)
def login(response: Response, req: LoginRequest):
    login_data = login_service(response, req)
    return login_data



@router.get("/logout", response_model=LogoutResponse)
def logout(response: Response, user=Depends(current_user_from_cookies), access_token: str = Cookie(None), refresh_token: str = Cookie(None)):
    return logout_service(response, access_token, refresh_token)



 





