# services/widgets_select_service.py
from typing import Any, Dict, Optional, List
from dto.widgets_select_dto import Module, WidgetType, WidgetSelection, WidgetDetail
from services.widgets_select_dal import fetch_widget_row, preview_via_rpc_or_table

TABLE_BY_TYPE = {
    "kpi": "TABLE_KPI",
    "chart": "TABLE_CHART",
    "table": "TABLE_TABLEAUX",
    "map": "TABLE_MAP",
}

def get_widget_detail_service(
    user: Dict[str, Any],
    module: Module,
    wtype: WidgetType,
    widget_id: str,
    selection: WidgetSelection,
    user_access_token: Optional[str],
) -> WidgetDetail:
    table = TABLE_BY_TYPE[wtype]
    # 1) récupérer la ligne du widget (config/catalogue) — doit contenir `rpc_name` si tu veux lier ta RPC
    row = fetch_widget_row(table=table, module=module, widget_id=widget_id, user_token=user_access_token)
    if not row:
        raise KeyError("Widget non trouvé")

    title = row.get("title") or row.get("name") or f"{wtype}:{widget_id}"
    base_config = row.get("config") or {}
    filters_schema = row.get("filters_schema") or None
    options_schema = row.get("options_schema") or None

    data_preview: Optional[List[Dict[str, Any]]] = None
    if selection.preview:
        data_preview = preview_via_rpc_or_table(row=row, selection=selection, user_token=user_access_token)

    merged_config = {**base_config}
    if selection.options:
        merged_config["options_overrides"] = selection.options

    return WidgetDetail(
        widget_id=row.get("id", widget_id),
        module=module,
        type=wtype,
        title=title,
        description=row.get("description"),
        config=merged_config,
        filters_schema=filters_schema,
        options_schema=options_schema,
        data_preview=data_preview
    )
