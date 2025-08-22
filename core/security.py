from fastapi import Response, Cookie, HTTPException, status, Depends
from services.auth_service import verify_token, refresh_access_token

# Dépendance centralisée
def get_current_user(
    response: Response,
    access_token: str = Cookie(None),
    refresh_token: str = Cookie(None)
):
    if not access_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing access token")

    try:
        return verify_token(access_token)
    except Exception:
        # Token expiré → on tente un refresh
        if not refresh_token:
            raise HTTPException(status_code=401, detail="Missing refresh token")

        session = refresh_access_token(refresh_token)
        if not session or not session.user:
            raise HTTPException(status_code=401, detail="Invalid refresh token")

        new_access_token = session.access_token
        user = session.user

        # ⚡ Mettre à jour cookie
        response.set_cookie(
            key="access_token",
            value=new_access_token,
            httponly=True,
            max_age=180,
            samesite="lax"
        )

        return {"id": user.id, "email": user.email, "access_token": new_access_token}
