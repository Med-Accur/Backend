import json
import os
import redis
from fastapi import HTTPException, status, Response
from dto.auth_dto import LoginRequest, LoginResponse, MeResponse, LogoutResponse, MultiRpcRequest
from core.config import supabase



# Initialisation Redis
redis_client = redis.from_url(
    os.getenv("REDIS_URL"),
    decode_responses=True
    #ssl=True   indispensable pour Redis Cloud (TLS)
)

ACCESS_TOKEN_TTL = 60 * 60  # 1 hour
REFRESH_TOKEN_TTL = 5 * 60 * 60  # 5 hours


def login_service(response: Response, req: LoginRequest) -> LoginResponse:
    auth_res = supabase.auth.sign_in_with_password({
        "email": req.email,
        "password": req.password
    })

    if not auth_res or not auth_res.session:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Login failed")

    user = auth_res.user
    user_cache = {"id": user.id, "email": user.email}
    redis_client.setex(f"token:{auth_res.session.access_token}", ACCESS_TOKEN_TTL, json.dumps(user_cache))
    redis_client.setex(f"refresh:{auth_res.session.refresh_token}", REFRESH_TOKEN_TTL, json.dumps(user_cache))

    response.set_cookie("access_token", auth_res.session.access_token, httponly=True, max_age=ACCESS_TOKEN_TTL, secure=True, samesite="none")
    response.set_cookie("refresh_token", auth_res.session.refresh_token, httponly=True, max_age=REFRESH_TOKEN_TTL, secure=True, samesite="none")
    return LoginResponse(
        id=user.id, 
        email=user.email
    )


def logout_service(response: Response, access_token: str = None, refresh_token: str = None) -> LogoutResponse:
    if not access_token and not refresh_token:
        raise HTTPException(status_code=401, detail="Aucun token fourni")
    access_exists = redis_client.exists(f"token:{access_token}")
    refresh_exists = redis_client.exists(f"refresh:{refresh_token}")

    if not access_exists and not refresh_exists:
        raise HTTPException(status_code=401, detail="session expirée")

    if access_token:
        redis_client.delete(f"token:{access_token}")
    if refresh_token:
        redis_client.delete(f"refresh:{refresh_token}")

    response.delete_cookie("access_token")
    response.delete_cookie("refresh_token")

    return LogoutResponse(message="Déconnexion réussie")




def verify_and_refresh_token_service(response: Response, access_token: str, refresh_token: str = None) -> dict:
    cached_user = redis_client.get(f"token:{access_token}")
    new_access_token = None
    new_refresh_token = None

    if cached_user:
        user_data = json.loads(cached_user)
        print("Access token valide")
    else:
        print("Access token expiré")
        if not refresh_token:
            raise HTTPException(status_code=401, detail="Token expiré, reconnectez-vous")

        cached_user = redis_client.get(f"refresh:{refresh_token}")
        if not cached_user:
            raise HTTPException(status_code=401, detail="Refresh token expiré")

        auth_res = supabase.auth.refresh_session(refresh_token)
        if not auth_res or not auth_res.session:
            raise HTTPException(status_code=401, detail="Impossible de rafraîchir le token")

        user = auth_res.user
        user_data = {"id": user.id, "email": user.email}

        redis_client.setex(f"token:{auth_res.session.access_token}", ACCESS_TOKEN_TTL, json.dumps(user_data))
        redis_client.setex(f"refresh:{auth_res.session.refresh_token}", REFRESH_TOKEN_TTL, json.dumps(user_data))

        new_access_token = auth_res.session.access_token
        new_refresh_token = auth_res.session.refresh_token
        print("Nouveau access_token et refresh_token générés via refresh")

    if new_access_token:
        response.set_cookie("access_token", new_access_token, httponly=True, max_age=ACCESS_TOKEN_TTL, samesite="lax", secure=False)
        print("Nouveau access_token mis à jour dans cookie")

    if new_refresh_token:
        response.set_cookie("refresh_token", new_refresh_token, httponly=True, max_age=REFRESH_TOKEN_TTL, samesite="lax", secure=False)
        print("Nouveau refresh_token mis à jour dans cookie")

    return user_data
