from pydantic import BaseModel
from typing import List, Dict, Optional, Any

class LoginRequest(BaseModel):
    email: str
    password: str

class MeResponse(BaseModel):
    id: str
    email: str
    kpi: Optional[List[Dict]] = None
    table: Optional[List[Dict]] = None
    chart: Optional[List[Dict]] = None
    maps: Optional[List[Dict]] = None

class LoginResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    id: str
    email: str


class LogoutResponse(BaseModel):
    message: str


class RpcRequest(BaseModel):
    widget_id: str
    rpc_name: str
    params: Optional[Dict[str, Any]] = None

class MultiRpcRequest(BaseModel):
    rpcs: List[RpcRequest]