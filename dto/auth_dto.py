from pydantic import BaseModel
from typing import List, Dict, Optional, Any, Union

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
    widgets: Optional[List[Dict]] = None

class LoginResponse(BaseModel):
    id: str
    email: str


class LogoutResponse(BaseModel):
    message: str


class RpcRequest(BaseModel):
    rpc_name: str
    params: Optional[Dict[str, Any]] = None

class MultiRpcRequest(BaseModel):
    rpcs: List[RpcRequest]

class Widget(BaseModel):
    key: str
    type: str
    x: float
    y: float
    w: float
    h: float
