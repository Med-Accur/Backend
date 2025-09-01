import json
import redis
from fastapi import HTTPException, status, Response
from dto.auth_dto import LoginRequest, LoginResponse, MeResponse, LogoutResponse, MultiRpcRequest
from core.config import supabase
from services.widgets_service import RPC_PYTHON_MAP


# Initialisation Redis
redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

ACCESS_TOKEN_TTL = 3600      
REFRESH_TOKEN_TTL = 3600*24*30     


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

    response.set_cookie("access_token", auth_res.session.access_token, httponly=True, max_age=ACCESS_TOKEN_TTL, samesite="lax", secure=False)
    response.set_cookie("refresh_token", auth_res.session.refresh_token, httponly=True, max_age=REFRESH_TOKEN_TTL, samesite="lax", secure=False)
    return LoginResponse(
        access_token=auth_res.session.access_token,
        refresh_token=auth_res.session.refresh_token,
        token_type="bearer",
        id=user.id, 
        email=user.email
    )


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


def me_service(response: Response, access_token: str, refresh_token: str = None) -> MeResponse:
    # Vérifie et refresh le token si nécessaire
    user_data = verify_and_refresh_token_service(response, access_token, refresh_token)

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





def get_widget_data(response, req, access_token, refresh_token=None):
    # Vérifier / refresh token
    user_data = verify_and_refresh_token_service(response, access_token, refresh_token)

    # Charger seulement les tables nécessaires
    tables = load_needed_tables(req.rpcs)

    results = {}
    for rpc in req.rpcs:
        try:
            if rpc.rpc_name in RPC_PYTHON_MAP:
                func = RPC_PYTHON_MAP[rpc.rpc_name]
                results[rpc.widget_id] = func(tables, **(rpc.params or {}))
            else:
                results[rpc.widget_id] = {"error": f"RPC {rpc.rpc_name} non défini"}
        except Exception as e:
            results[rpc.widget_id] = {"error": str(e)}

    return results


def load_needed_tables(rpcs):
    needed_tables = set()
    for rpc in rpcs:
        needed_tables.update(WIDGET_DEPENDENCIES.get(rpc.rpc_name, []))

    loaded = {}
    for table in needed_tables:
        cache_key = f"table_cache:{table}"
        cached = redis_client.get(cache_key)

        if cached:
            print(f"[CACHE] Table {table} récupérée depuis Redis")
            loaded[table] = json.loads(cached)
        else:
            print(f"[SUPABASE] Chargement table {table}")
            res = supabase.table(table).select("*").execute()
            data = res.data or []

            redis_client.setex(cache_key, 300, json.dumps(data))  # stock avec expiration
            loaded[table] = data

    return loaded

WIDGET_DEPENDENCIES = {
    "get_table_cmd_clients": ["commandeclient", "contact"],
    "get_change_log": ["changelog"],
    "kpi_nb_commandes": ["commandeclient"],
    "kpi_taux_retards": ["commandeclient"],
    "kpi_otif": ["commandeclient"],
    "kpi_taux_annulation": ["commandeclient"],
    "kpi_duree_cycle_moyenne_jours": ["commandeclient"],
    # autres...
}
