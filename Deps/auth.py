from fastapi import Cookie, Response
from services.auth_service import verify_and_refresh_token_service

def current_user_from_cookies(response: Response, access_token: str = Cookie(None), refresh_token: str = Cookie(None)):
    return verify_and_refresh_token_service(response, access_token, refresh_token)