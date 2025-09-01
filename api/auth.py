from fastapi import APIRouter, Depends, Response, Cookie, Request, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from dto.auth_dto import LoginRequest, LoginResponse, MeResponse, LogoutResponse, MultiRpcRequest
from services.auth_service import login_service, me_service, logout_service, get_widget_data
import redis


router = APIRouter(prefix="/api/auth", tags=["auth"])
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")


@router.post("/login", response_model=LoginResponse)
def login(response: Response, req: LoginRequest):
    login_data = login_service(response, req)
    return login_data



@router.get("/config/me", response_model=MeResponse)
def me(response: Response, token: str = Depends(oauth2_scheme), refresh_token: str = Cookie(None)):
    me_data = me_service(response, token, refresh_token)
    return me_data


@router.post("/{module}/widgets")
def get_widgets(response: Response, req: MultiRpcRequest, token: str = Depends(oauth2_scheme), refresh_token: str = Cookie(None)):
    me_data = get_widget_data(response, req, token, refresh_token)
    return me_data


@router.post("/logout", response_model=LogoutResponse)
def logout(response: Response, token: str = Depends(oauth2_scheme), refresh_token: str = Cookie(None)):
    return logout_service(response, token, refresh_token)


redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

@router.post("/webhook/cache_invalidate")
async def cache_invalidate(req: Request):
    """
    Webhook appelé par Supabase lorsqu'une table change.
    Body attendu: {"table": "<nom_table>"}
    """
    
    
    data = await req.json()
    print(f"[CACHE] Invalidation request received: {data}")
    table = data.get("table")

    if not table:
        raise HTTPException(status_code=400, detail="Table not specified")

    # Supprimer la clé cache correspondante
    key = f"table_cache:{table}"
    redis_client.delete(key)
    print(f"[CACHE] Cache supprimé pour la table: {table}")

    return {"status": "ok", "deleted_key": key}
