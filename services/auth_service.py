import json
import redis
from fastapi import HTTPException, status, Response
from dto.auth_dto import LoginRequest, LoginResponse, MeResponse
from core.config import supabase

# Initialisation Redis
redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

# ⚡ Durées réduites pour tester
ACCESS_TOKEN_TTL = 3600       # 1 heure
REFRESH_TOKEN_TTL = 3600*24*30     # 30 jours


def login_service(req: LoginRequest) -> LoginResponse:
    auth_res = supabase.auth.sign_in_with_password({
        "email": req.email,
        "password": req.password
    })

    if not auth_res or not auth_res.session:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Login failed")

    user = auth_res.user
    user_cache = {"id": user.id, "email": user.email}

    # Stocker access_token et refresh_token
    redis_client.setex(f"token:{auth_res.session.access_token}", ACCESS_TOKEN_TTL, json.dumps(user_cache))
    redis_client.setex(f"refresh:{auth_res.session.refresh_token}", REFRESH_TOKEN_TTL, json.dumps(user_cache))

    return LoginResponse(
        access_token=auth_res.session.access_token,
        refresh_token=auth_res.session.refresh_token,
        token_type="bearer",
        user=MeResponse(id=user.id, email=user.email)
    )


def verify_and_refresh_token_service(response: Response, access_token: str, refresh_token: str = None) -> dict:
    """
    Vérifie l'access_token et refresh si nécessaire.
    Met à jour Redis et les cookies.
    Retourne les données utilisateur.
    """
    cached_user = redis_client.get(f"token:{access_token}")
    new_access_token = None
    new_refresh_token = None

    if cached_user:
        user_data = json.loads(cached_user)
        print("[TOKEN] Access token valide ✅")
    else:
        print("[TOKEN] Access token expiré ❌")
        if not refresh_token:
            raise HTTPException(status_code=401, detail="Token expiré, reconnectez-vous")

        cached_user = redis_client.get(f"refresh:{refresh_token}")
        if not cached_user:
            raise HTTPException(status_code=401, detail="Refresh token expiré")

        # Rafraîchir via Supabase
        auth_res = supabase.auth.refresh_session(refresh_token)
        if not auth_res or not auth_res.session:
            raise HTTPException(status_code=401, detail="Impossible de rafraîchir le token")

        user = auth_res.user
        user_data = {"id": user.id, "email": user.email}

        # Stocker les nouveaux tokens dans Redis
        redis_client.setex(f"token:{auth_res.session.access_token}", ACCESS_TOKEN_TTL, json.dumps(user_data))
        redis_client.setex(f"refresh:{auth_res.session.refresh_token}", REFRESH_TOKEN_TTL, json.dumps(user_data))

        new_access_token = auth_res.session.access_token
        new_refresh_token = auth_res.session.refresh_token
        print("[TOKEN] Nouveau access_token et refresh_token générés via refresh 🔄")

    # Mettre à jour les cookies si nécessaire
    if new_access_token:
        response.set_cookie("access_token", new_access_token, httponly=True, max_age=ACCESS_TOKEN_TTL, samesite="lax", secure=False)
        print(f"[COOKIE] Nouveau access_token mis à jour dans cookie: {new_access_token}")

    if new_refresh_token:
        response.set_cookie("refresh_token", new_refresh_token, httponly=True, max_age=REFRESH_TOKEN_TTL, samesite="lax", secure=False)
        print(f"[COOKIE] Nouveau refresh_token mis à jour dans cookie: {new_refresh_token}")

    return user_data


def me_service(response: Response, access_token: str, refresh_token: str = None) -> MeResponse:
    """
    Service complet pour /me.
    Vérifie les tokens, refresh si nécessaire, récupère les données utilisateur.
    """
    # Vérifie et refresh le token si nécessaire
    user_data = verify_and_refresh_token_service(response, access_token, refresh_token)

    # Récupérer les données utilisateur depuis Supabase
    user_id = user_data["id"]
    kpi_res = supabase.table("TABLE_KPI").select("*").execute()
    table_res = supabase.table("TABLE_TABLEAUX").select("*").execute()
    chart_res = supabase.table("TABLE_CHART").select("*").execute()
    map_res = supabase.table("TABLE_MAP").select("*").execute()


    return MeResponse(
        id=user_id,
        email=user_data["email"],
        kpi=kpi_res.data,
        table=table_res.data,
        chart=chart_res.data,
        maps=map_res.data
    )
