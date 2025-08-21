from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from services.auth_service import me_service

security = HTTPBearer()

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    try:
        return me_service(token)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
