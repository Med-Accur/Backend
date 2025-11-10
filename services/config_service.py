import os
from fastapi import HTTPException
import json
import redis
from services.widgets_service import RPC_PYTHON_MAP
from core.config import supabase
from dto.auth_dto import MeResponse 

# Initialisation Redis
redis_client = redis.from_url(
    os.getenv("REDIS_URL"),
    decode_responses=True
    #ssl=True  # indispensable pour Redis Cloud (TLS)
)

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
    widget_res = supabase.table("dash_widgets").select("*").eq("user_id", user_id).execute()

    return MeResponse(
        id=user_id,
        email=user_data["email"],
        kpi=kpi_res.data,
        table=table_res.data,
        chart=chart_res.data,
        maps=map_res.data,
        widgets=widget_res.data
    )


def post_widget(response, req, user, module):
    try:
        user_id = user["id"]
        print(f"Enregistrement des widgets pour user {user_id} module '{module}'")
        existing = (
            supabase.table("dash_widgets")
            .select("id")
            .eq("user_id", user_id)
            .eq("dashboard", module)
            .execute()
        )

        if existing.data and len(existing.data) > 0:
            print(f"Config existante pour user {user_id} module '{module}', on la remplace…")

            supabase.table("dash_widgets").delete().eq("user_id", user_id).eq("dashboard", module).execute()

        inserts = [
            {
                "user_id": user_id,
                "dashboard": module,
                "widget_key": w.key,
                "widget_type": w.type,
                "x": w.x,
                "y": w.y,
                "w": w.w,
                "h": float(w.h),
            }
            for w in req
        ]

        if not inserts:
            return {"status": "no_data", "inserted": 0}

        data = supabase.table("dash_widgets").insert(inserts).execute()
        print(f"Widgets enregistrés pour user {user_id} module '{module}'")
        return {
            "status": "success",
            "dashboard": module,
            "inserted": len(data.data)
        }

    except Exception as e:
        print("Erreur lors de l’enregistrement des widgets:", e)
        raise HTTPException(status_code=500, detail=str(e))



def get_widget_data(response, req):
    tables = load_needed_tables(req.rpcs)
    print(f"params: {req.rpcs}")
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
    "chart_duree_changelog": ["changelog"],
    "kpi_duree_moyenne_changelog": ["changelog"],
    "chart_duree_cycle_moyenne": ["commandeclient", "modelivraison"],
    "chart_taux_annulation": ["commandeclient", "modelivraison"],
    "chart_otif": ["commandeclient","modelivraison"],
    "chart_taux_retard": ["commandeclient","modelivraison"],
    "chart_commandes_client": ["commandeclient","modelivraison"],
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
    "rpc_orders_chart": ["commandeclient"],
    "kpi_quantite_stock": ["stock"],
    "kpi_quantite_reservee": ["stock"],
    "kpi_stock_disponible": ["stock"],
    "kpi_days_on_hand": ["stock", "mouvement_stock"],
    "kpi_taux_rotation": ["stock", "commandeclient", "lignecommande"],
    "kpi_inventory_to_sales": ["stock", "commandeclient", "lignecommande", "produit"],
    "kpi_rentabilite_stock": ["stock", "commandeclient", "lignecommande", "produit"],
    "kpi_taux_rupture": ["stock", "mouvement_stock"],
    "kpi_remaining_shelf_life_avg": ["stock", "produit"],
    "kpi_produits_proches_peremption": ["stock", "produit"],
    "kpi_contraction_stock_qte": ["stock", "mouvement_stock"],
    "kpi_sup_on_time_rate": ["commande_fournisseur", "ligne_cmd_fournisseur", "reception_fournisseur"],
    "kpi_sup_quality_conform_rate": ["reception_fournisseur", "ligne_cmd_fournisseur", "commande_fournisseur"],
    "kpi_sup_quality_nonconform_rate": ["reception_fournisseur", "ligne_cmd_fournisseur", "commande_fournisseur"],
    "kpi_sup_return_rate": ["reception_fournisseur", "retour_fournisseur"],
    "kpi_sup_avg_lead_time_days": ["commande_fournisseur", "reception_fournisseur"],
    "kpi_sup_transport_cost_ratio": ["commande_fournisseur", "ligne_cmd_fournisseur", "reception_fournisseur"],
     "rpc_stock_disponible_series": ["stock"],
    "rpc_days_on_hand_series": ["stock", "mouvement_stock"],
    "rpc_taux_rotation_series": ["stock", "commandeclient", "lignecommande", "produit"],
    "rpc_inventory_to_sales_series": ["stock", "commandeclient", "lignecommande", "produit"],
    "rpc_rentabilite_stock_series": ["stock", "commandeclient", "lignecommande", "produit"],
    "rpc_taux_rupture_series": ["stock", "mouvement_stock", "commandeclient", "lignecommande"],
    # ⚠️ Cette série lit la table "peremption"
    "rpc_remaining_shelf_life_series": ["peremption"],
    "rpc_shrinkage_by_day": ["mouvement_stock", "stock"],
   "rpc_sup_on_time_rate_series": ["commande_fournisseur", "reception_fournisseur"],
    "rpc_sup_quality_conform_rate_series": ["reception_fournisseur"],
    "rpc_sup_quality_nonconform_rate_series": ["ligne_cmd_fournisseur", "commande_fournisseur"],
    "rpc_sup_return_rate_series": ["reception_fournisseur", "retour_fournisseur"],
    "rpc_sup_avg_lead_time_days_series": ["commande_fournisseur", "reception_fournisseur"],
    "rpc_sup_transport_cost_ratio_series": ["commande_fournisseur"],
}
