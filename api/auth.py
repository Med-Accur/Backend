from fastapi import APIRouter, Depends, Response, Cookie
from fastapi.security import OAuth2PasswordBearer
from dto.auth_dto import LoginRequest, LoginResponse, MeResponse
from services.auth_service import login_service, me_service

router = APIRouter(prefix="/api/auth", tags=["auth"])
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")


@router.post("/login", response_model=LoginResponse)
def login(req: LoginRequest, response: Response):
    login_data = login_service(req)

    # Cookie HTTPOnly pour access_token et refresh_token
    response.set_cookie(
        key="access_token",
        value=login_data.access_token,
        httponly=True,
        max_age=3600,  # 1 heure
        samesite="lax",
        secure=False
    )
    response.set_cookie(
        key="refresh_token",
        value=login_data.refresh_token,
        httponly=True,
        max_age=3600*24*30,  # 30 jours
        samesite="lax",
        secure=False
    )
    return login_data



@router.get("/config/me", response_model=MeResponse)
def me(response: Response, token: str = Depends(oauth2_scheme), refresh_token: str = Cookie(None)):
    """
    token: access_token depuis Authorization header
    refresh_token: depuis cookie httpOnly
    """
    # Le service gère déjà la vérification, le refresh et la mise à jour des cookies
    me_data = me_service(response, token, refresh_token)
    return me_data
