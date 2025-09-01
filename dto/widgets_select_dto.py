# dto/widgets_select_dto.py
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Literal, Optional

Module = Literal["accueil", "commandeclient", "fournisseur", "stock"]
WidgetType = Literal["kpi", "chart", "table", "map"]

class WidgetSelection(BaseModel):
    filters: Optional[Dict[str, Any]] = None
    options: Optional[Dict[str, Any]] = None
    preview: bool = True
    preview_limit: int = Field(default=50, ge=1, le=500)

class WidgetDetail(BaseModel):
    widget_id: str
    module: Module
    type: WidgetType
    title: str
    description: Optional[str] = None
    config: Dict[str, Any]
    filters_schema: Optional[List[Dict[str, Any]]] = None
    options_schema: Optional[List[Dict[str, Any]]] = None
    data_preview: Optional[List[Dict[str, Any]]] = None
