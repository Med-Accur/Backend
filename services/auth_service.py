from dto.auth_dto import LoginRequest, LoginResponse, MeResponse
from core.config import supabase
import redis
import json

# Initialisation de Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

def login_service(req: LoginRequest) -> LoginResponse:
    # Authentification via Supabase
    auth_res = supabase.auth.sign_in_with_password({
        "email": req.email,
        "password": req.password
    })

    if not auth_res or not auth_res.session:
        raise Exception("Login failed")

    user = auth_res.user

    # Préparer les données à mettre en cache
    user_cache = {
        "id": user.id,
        "email": user.email
    }

    # Stocker les infos utilisateur avec TTL de 1h
    redis_client.setex(f"user:{user.id}", 3600, json.dumps(user_cache))

    # Stocker le token et le refresh_token en Redis
    redis_client.setex(f"token:{auth_res.session.access_token}", 3600, json.dumps(user_cache))
    redis_client.setex(f"refresh:{auth_res.session.access_token}", 60*60*24*30, auth_res.session.refresh_token)
    
    return LoginResponse(
        access_token=auth_res.session.access_token,
        refresh_token=auth_res.session.refresh_token,
        token_type="bearer",
        user=MeResponse(id=user.id, email=user.email)
    )

def me_service(access_token: str) -> MeResponse:
    # Vérifie token et récupère infos utilisateur
    data = verify_token(access_token)
    user_id = data["id"]

    # Récupérer données depuis Supabase
    kpi_res = supabase.table("TABLE_KPI").select("*").execute()
    table_res = supabase.table("TABLE_TABLEAUX").select("*").execute()
    chart_res = supabase.table("TABLE_CHART").select("*").execute()
    map_res = supabase.table("TABLE_MAP").select("*").execute()

    if not kpi_res.data and not table_res.data and not chart_res.data and not map_res.data:
        raise Exception("Aucune donnée trouvée pour cet utilisateur")

    return MeResponse(
        id=data["id"],
        email=data["email"],
        kpi=kpi_res.data,
        table=table_res.data,
        chart=chart_res.data,
        maps=map_res.data
    )


def refresh_access_token(refresh_token: str):
    """
    Rafraîchit un access_token via Supabase à partir d'un refresh_token
    """
    refreshed = supabase.auth.refresh_session(refresh_token)
    if not refreshed or not refreshed.session:
        raise Exception("Refresh token invalide")
    return refreshed.session


def verify_token(access_token: str) -> dict:
    """
    Vérifie le token dans Redis, le rafraîchit si expiré,
    et retourne les infos utilisateur valides + token.
    """
    
    cached_user = redis_client.get(f"token:{access_token}")
    if cached_user:
        data = json.loads(cached_user)
        data["access_token"] = access_token  
        return data
        

    
    try:
        user_res = supabase.auth.get_user(access_token)
        if not user_res or not user_res.user:
            raise Exception("Token invalide")
        user = user_res.user
        data = {"id": user.id, "email": user.email, "access_token": access_token}

        
        redis_client.setex(f"token:{access_token}", 3600, json.dumps({"id": user.id, "email": user.email}))
        return data
    except Exception as e:
       
        refresh_token = redis_client.get(f"refresh:{access_token}")
        if refresh_token:
            session = refresh_access_token(refresh_token)
            new_access_token = session.access_token
            user = session.user
            data = {"id": user.id, "email": user.email, "access_token": new_access_token}

            
            redis_client.setex(f"token:{new_access_token}", 3600, json.dumps({"id": user.id, "email": user.email}))
            redis_client.setex(f"refresh:{new_access_token}", 3600*24*30, refresh_token)
        
            return data

        raise Exception("Token expiré, veuillez vous reconnecter")
