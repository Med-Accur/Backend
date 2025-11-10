from fastapi import APIRouter, Depends, Response, Cookie
from dto.auth_dto import LoginRequest, LoginResponse, MeResponse, Widget, MultiRpcRequest
from Deps.auth import current_user_from_cookies
from services.config_service import me_service, get_widget_data, post_widget



router = APIRouter(prefix="/api/pre", tags=["pre"])

@router.get("/config/me", response_model=MeResponse)
def me(user=Depends(current_user_from_cookies)):
    me_data = me_service(user)
    return me_data


@router.post("/{module}/widgets")
def get_widgets(response: Response, req: MultiRpcRequest, user=Depends(current_user_from_cookies)):
    me_data = get_widget_data(response, req)
    return me_data

@router.post("/config/me/{module}/widgets")
def post_widgets(module: str, response: Response, req: list[Widget], user=Depends(current_user_from_cookies)):
    me_data = post_widget(response, req, user, module=module)
    return me_data

