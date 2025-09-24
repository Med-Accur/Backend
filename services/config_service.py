import json
import redis
from services.widgets_service import RPC_PYTHON_MAP
from core.config import supabase
from dto.auth_dto import MeResponse 

# Initialisation Redis
redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


def me_service(user_data: dict) -> MeResponse:
    """
    Reçoit directement user_data (déjà authentifié via la dépendance).
    Ne touche PAS aux cookies ni au refresh.
    """
    user_id = user_data["id"]

    kpi_res   = supabase.table("TABLE_KPI").select("*").execute()
    table_res = supabase.table("TABLE_TABLEAUX").select("*").execute()
    chart_res = supabase.table("TABLE_CHART").select("*").execute()
    map_res   = supabase.table("TABLE_MAP").select("*").execute()

    return MeResponse(
        id=user_id,
        email=user_data["email"],
        kpi=kpi_res.data,
        table=table_res.data,
        chart=chart_res.data,
        maps=map_res.data
    )

def get_widget_data(response, req):
    # Charger seulement les tables nécessaires
    tables = load_needed_tables(req.rpcs)

    results = {}
    for rpc in req.rpcs:
        try:
            if rpc.rpc_name in RPC_PYTHON_MAP:
                func = RPC_PYTHON_MAP[rpc.rpc_name]
                results[rpc.rpc_name] = func(tables, **(rpc.params or {}))
            else:
                results[rpc.rpc_name] = {"error": f"RPC {rpc.rpc_name} non défini"}
        except Exception as e:
            results[rpc.rpc_name] = {"error": str(e)}

    return results


def load_needed_tables(rpcs):
    needed_tables = set()
    for rpc in rpcs:
        needed_tables.update(WIDGET_DEPENDENCIES.get(rpc.rpc_name, []))

    loaded = {}
    for table in needed_tables:
        cache_key = f"table_cache:{table}"
        cached = redis_client.get(cache_key)

        if cached:
            print(f"[CACHE] Table {table} récupérée depuis Redis")
            loaded[table] = json.loads(cached)
        else:
            print(f"[SUPABASE] Chargement table {table}")
            res = supabase.table(table).select("*").execute()
            data = res.data or []

            redis_client.setex(cache_key, 300, json.dumps(data))  
            loaded[table] = data

    return loaded

WIDGET_DEPENDENCIES = {
    "get_table_cmd_clients": ["commandeclient", "contact"],
    "get_table_change_log": ["changelog"],
    "kpi_nb_commandes": ["commandeclient"],
    "kpi_taux_retards": ["commandeclient"],
    "kpi_otif": ["commandeclient"],
    "kpi_taux_annulation": ["commandeclient"],
    "kpi_duree_cycle_moyenne_jours": ["commandeclient"],
    "kpi_volume_production": ["ordre_fabrication"],
    "kpi_uph": ["production_log"],
    "kpi_temps_cycle": ["production_log"],
    "kpi_taux_conformite": ["controle_qualite"],
    "kpi_taux_defauts": ["production_log"],
    "kpi_taux_retouche": ["production_log"],
    "kpi_lead_time": ["ordre_fabrication"],
    "kpi_respect_planning": ["ordre_fabrication"],
    "kpi_adherence_plan": ["ordre_fabrication"],
    "kpi_wip": ["ordre_fabrication"],
    "kpi_trs": ["production_log", "machine"],
    "kpi_productivite": ["production_rh", "temps_travail", "employe"],
    "kpi_ecart_charge": ["production_rh", "temps_travail"],
    "kpi_taux_utilisation": ["temps_travail"],
    "kpi_efficacite": ["production_rh"],
    "kpi_cout_horaire_unite": ["production_rh", "temps_travail", "employe"],
    "kpi_taux_erreur": ["production_rh"],
    "kpi_taux_recyclage": ["dechet"],
    # autres...
}
