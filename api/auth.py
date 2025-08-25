from fastapi import APIRouter, Depends, HTTPException, status, Response
from fastapi.security import OAuth2PasswordBearer
from services.auth_service import login_service, me_service, verify_token
from dto.auth_dto import LoginRequest, LoginResponse, MeResponse

router = APIRouter(prefix="/api/auth", tags=["auth"])


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")


@router.post("/login", response_model=LoginResponse)
def login(req: LoginRequest, response: Response):
    try:
        login_data = login_service(req)
        print(login_data.access_token)
       
        response.set_cookie(
            key="access_token",
            value=login_data.access_token,
            httponly=True,
            max_age=3600,
            samesite="lax"
        )
        return login_data
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))


@router.get("/config/me", response_model=MeResponse)
def get_me(response: Response, token: str = Depends(oauth2_scheme)):
    try:
      
        result = verify_token(token)

      
        if isinstance(result, str):
           
            response.set_cookie(
                key="access_token",
                value=result,
                httponly=True,
                max_age=3600,
                samesite="lax"
            )
            token_to_use = result  
        else:
            token_to_use = token  

        
        return me_service(token_to_use)

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))
