# api/widgets_select_router.py
from fastapi import APIRouter, Depends, Response, Cookie, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from typing import Optional
from dto.widgets_select_dto import Module, WidgetType, WidgetSelection, WidgetDetail
from services.widgets_select_service import get_widget_detail_service
from services.auth_service import verify_and_refresh_token_service

router = APIRouter(prefix="/api", tags=["widgets"])
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")

@router.post("/{module}/widgets/{widget}/{widget_id}", response_model=WidgetDetail)
def select_widget(
    module: Module,
    widget: WidgetType,
    widget_id: str,
    payload: WidgetSelection,
    response: Response,
    token: str = Depends(oauth2_scheme),
    refresh_cookie: Optional[str] = Cookie(None, alias="refresh_token"),
):
    # Auth + refresh (réutilise ta logique existante)
    user = verify_and_refresh_token_service(response, token, refresh_cookie)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

    return get_widget_detail_service(
        user=user,
        module=module,
        wtype=widget,
        widget_id=widget_id,
        selection=payload,
        user_access_token=token  # => RLS ON côté Supabase
    )
