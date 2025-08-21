from dto.auth_dto import LoginRequest, LoginResponse, MeResponse
from core.config import supabase

def login_service(req: LoginRequest) -> LoginResponse:
    auth_res = supabase.auth.sign_in_with_password({
        "email": req.email,
        "password": req.password
    })

    if not auth_res or not auth_res.session:
        raise Exception("Login failed")

    return LoginResponse(
        access_token=auth_res.session.access_token,
        refresh_token=auth_res.session.refresh_token
    )

def me_service(access_token: str) -> MeResponse:
    user = supabase.auth.get_user(access_token)
    if not user or not user.user:
        raise Exception("Invalid token")

    return MeResponse(
        id=user.user.id,
        email=user.user.email
    )
