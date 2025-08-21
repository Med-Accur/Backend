from fastapi import APIRouter, Depends
from dto.auth_dto import LoginRequest, LoginResponse, MeResponse
from services.auth_service import login_service
from core.security import get_current_user

router = APIRouter(prefix="/auth", tags=["auth"])

@router.post("/login", response_model=LoginResponse)
def login(req: LoginRequest):
    return login_service(req)

@router.get("/me", response_model=MeResponse)
def me(user: MeResponse = Depends(get_current_user)):
    return user
