from fastapi import APIRouter, Depends, Response, Cookie
from dto.auth_dto import LoginRequest, LoginResponse, MeResponse, LogoutResponse, MultiRpcRequest
from Deps.auth import current_user_from_cookies
from services.auth_service import login_service, logout_service
from services.config_service import me_service, get_widget_data
import redis
import json

router = APIRouter(prefix="/api/auth", tags=["auth"])

# Redis client (synchrone)
redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)



@router.post("/login", response_model=LoginResponse)
def login(response: Response, req: LoginRequest):
    login_data = login_service(response, req)
    return login_data



@router.get("/logout", response_model=LogoutResponse)
def logout(response: Response, user=Depends(current_user_from_cookies), access_token: str = Cookie(None), refresh_token: str = Cookie(None)):
    return logout_service(response, access_token, refresh_token)



 





