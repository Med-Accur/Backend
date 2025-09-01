# services/widgets_select_dal.py
from typing import Any, Dict, List, Optional
from core.config import supabase
import datetime

def _with_user_context(user_token: Optional[str]):
    if user_token:
        supabase.postgrest.auth(user_token)  # propage le JWT → RLS ON
    return supabase

def fetch_widget_row(table: str, module: str, widget_id: str, user_token: Optional[str]) -> Optional[Dict[str, Any]]:
    client = _with_user_context(user_token)
    res = client.table(table).select("*").eq("module", module).eq("id", widget_id).limit(1).execute()
    rows = res.data or []
    return rows[0] if rows else None

def preview_via_rpc_or_table(row: Dict[str, Any], selection, user_token: Optional[str]) -> List[Dict[str, Any]]:
    client = _with_user_context(user_token)
    preview_limit = selection.preview_limit or 50
    f = selection.filters or {}

    # 1) Si le widget référence une RPC (recommandé pour KPI & tables complexes)
    rpc_name = row.get("rpc_name")
    if rpc_name:
        # Mappe automatiquement tes filtres vers les paramètres de RPC
        # Exemple pour tes RPC de TABLES :
        params = {
            # get_table_change_log / get_table_cmd_clients
            "date_min": f.get("date_min"),
            "date_max": f.get("date_max"),
            "statut_filter": f.get("statut") or f.get("statut_filter"),
            "client_filter": f.get("client") or f.get("client_filter"),
        }
        # Purge les None non utilisés
        params = {k: v for k, v in params.items() if v is not None}

        data = client.rpc(rpc_name, params=params).execute().data or []
        return data[:preview_limit]

    # 2) Sinon, SELECT direct sur une table source (si définie)
    src = row.get("source_table")
    if not src:
        return []

    q = client.table(src).select("*")
    if "date_min" in f: q = q.gte("date", f["date_min"])
    if "date_max" in f: q = q.lte("date", f["date_max"])
    if "statut" in f:   q = q.eq("statut", f["statut"])
    if "client" in f:   q = q.ilike("client", f"%{f['client']}%")
    q = q.limit(preview_limit)

    return q.execute().data or []
