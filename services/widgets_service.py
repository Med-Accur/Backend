# services/widgets_service.py
from __future__ import annotations
from collections import defaultdict
from services.production.kpi import (
    get_kpi_adherence_plan, 
    get_kpi_lead_time, 
    get_kpi_respect_planning, 
    get_kpi_taux_conformite, 
    get_kpi_taux_defauts, 
    get_kpi_taux_retouche, 
    get_kpi_temps_cycle,
    get_kpi_trs, 
    get_kpi_uph, 
    get_kpi_volume_production, 
    get_kpi_wip
)
from services.cap_charge.kpi import (
    get_kpi_productivite,
    get_kpi_ecart_charge,
    get_kpi_taux_utilisation,
    get_kpi_efficacite,
    get_kpi_cout_horaire_unite,
    get_kpi_taux_erreur,
    get_kpi_taux_recyclage
)
from services.cmd_client.kpi import (

    get_kpi_duree_moyenne_changelog,
    get_kpi_nb_commandes,
    get_kpi_taux_retards,
    get_kpi_otif,
    get_kpi_taux_annulation,
    get_kpi_duree_cycle_moyenne_jours
)

from services.stock.kpi import (
    get_kpi_quantite_stock,
    get_kpi_quantite_reservee,
    get_kpi_stock_disponible,
    get_kpi_days_on_hand,
    get_kpi_taux_rotation,
    get_kpi_inventory_to_sales,
    get_kpi_rentabilite_stock,
    get_kpi_taux_rupture,
    get_kpi_remaining_shelf_life_avg,
    get_kpi_produits_proches_peremption,
    get_kpi_contraction_stock_qte,
)
from services.fournisseur.kpi import (
    get_sup_on_time_rate,
    get_sup_quality_conform_rate,
    get_sup_quality_nonconform_rate,
    get_sup_return_rate,
    get_sup_avg_lead_time_days,
    get_sup_transport_cost_ratio,
)





# services/widgets_service.py (extrait : imports à ajouter)

from services.cmd_client.chart import (
    rpc_nb_otif,
    rpc_taux_retard,
    rpc_duree_cycle_moyenne,
    rpc_orders_chart,
    rpc_taux_annulation,
    rpc_duree_changelog_chart,
    get_table_cmd_clients_service,
    get_change_log,
)

from services.stock.chart import (
    rpc_stock_disponible_series,
    rpc_days_on_hand_series,
    rpc_taux_rotation_series,
    rpc_inventory_to_sales_series,
    rpc_rentabilite_stock_series,
    rpc_taux_rupture_series,
    rpc_remaining_shelf_life_series,
    rpc_shrinkage_by_day,
)

from services.fournisseur.chart import (
    rpc_sup_on_time_rate_series,
    rpc_sup_quality_conform_rate_series,
    rpc_sup_quality_nonconform_rate_series,
    rpc_sup_return_rate_series,
    rpc_sup_avg_lead_time_days_series,
    rpc_sup_transport_cost_ratio_series,
)


RPC_PYTHON_MAP = {
    "get_kpi_duree_moyenne_changelog": get_kpi_duree_moyenne_changelog,
    "rpc_duree_cycle_moyenne": rpc_duree_cycle_moyenne,
    "rpc_taux_annulation": rpc_taux_annulation,
    "rpc_nb_otif": rpc_nb_otif,
    "rpc_taux_retard": rpc_taux_retard,
    "rpc_orders_chart": rpc_orders_chart,
    "get_table_cmd_clients": get_table_cmd_clients_service,
    "kpi_volume_production": get_kpi_volume_production,
    "kpi_taux_conformite": get_kpi_taux_conformite,
    "kpi_taux_defauts": get_kpi_taux_defauts,
    "kpi_taux_retouche": get_kpi_taux_retouche,
    "kpi_uph": get_kpi_uph,
    "kpi_temps_cycle": get_kpi_temps_cycle,
    "get_table_change_log": get_change_log,
    "kpi_nb_commandes": get_kpi_nb_commandes,
    "kpi_taux_retards": get_kpi_taux_retards,
    "kpi_otif": get_kpi_otif,
    "kpi_taux_annulation": get_kpi_taux_annulation,
    "kpi_duree_cycle_moyenne_jours": get_kpi_duree_cycle_moyenne_jours,
    "kpi_lead_time": get_kpi_lead_time,
    "kpi_respect_planning": get_kpi_respect_planning,
    "kpi_adherence_plan": get_kpi_adherence_plan,
    "kpi_wip": get_kpi_wip,
    "kpi_trs": get_kpi_trs,
    "kpi_productivite": get_kpi_productivite,
    "kpi_ecart_charge": get_kpi_ecart_charge,
    "kpi_taux_utilisation": get_kpi_taux_utilisation,
    "kpi_efficacite": get_kpi_efficacite,
    "kpi_cout_horaire_unite": get_kpi_cout_horaire_unite,
    "kpi_taux_erreur": get_kpi_taux_erreur,
    "kpi_taux_recyclage": get_kpi_taux_recyclage,
    "kpi_quantite_stock": get_kpi_quantite_stock,
    "kpi_quantite_reservee": get_kpi_quantite_reservee,
    "kpi_stock_disponible": get_kpi_stock_disponible,
    "kpi_days_on_hand": get_kpi_days_on_hand,
    "kpi_taux_rotation": get_kpi_taux_rotation,
    "kpi_inventory_to_sales": get_kpi_inventory_to_sales,
    "kpi_rentabilite_stock": get_kpi_rentabilite_stock,
    "kpi_taux_rupture": get_kpi_taux_rupture,
    "kpi_remaining_shelf_life_avg": get_kpi_remaining_shelf_life_avg,
    "kpi_produits_proches_peremption": get_kpi_produits_proches_peremption,
    "kpi_contraction_stock_qte": get_kpi_contraction_stock_qte,
    "kpi_sup_on_time_rate": get_sup_on_time_rate,
    "kpi_sup_quality_conform_rate": get_sup_quality_conform_rate,
    "kpi_sup_quality_nonconform_rate": get_sup_quality_nonconform_rate,
    "kpi_sup_return_rate": get_sup_return_rate,
    "kpi_sup_avg_lead_time_days": get_sup_avg_lead_time_days,
    "kpi_sup_transport_cost_ratio": get_sup_transport_cost_ratio,
    "get_kpi_duree_moyenne_changelog": get_kpi_duree_moyenne_changelog,
    "rpc_duree_changelog_chart": rpc_duree_changelog_chart,
    
    "rpc_stock_disponible_series": rpc_stock_disponible_series,
    "rpc_days_on_hand_series": rpc_days_on_hand_series,
    "rpc_taux_rotation_series": rpc_taux_rotation_series,
    "rpc_inventory_to_sales_series": rpc_inventory_to_sales_series,
    "rpc_rentabilite_stock_series": rpc_rentabilite_stock_series,
    "rpc_taux_rupture_series": rpc_taux_rupture_series,
    "rpc_remaining_shelf_life_series": rpc_remaining_shelf_life_series,
    "rpc_shrinkage_by_day": rpc_shrinkage_by_day,
    
     "rpc_sup_on_time_rate_series": rpc_sup_on_time_rate_series,
    "rpc_sup_quality_conform_rate_series": rpc_sup_quality_conform_rate_series,
    "rpc_sup_quality_nonconform_rate_series": rpc_sup_quality_nonconform_rate_series,
    "rpc_sup_return_rate_series": rpc_sup_return_rate_series,
    "rpc_sup_avg_lead_time_days_series": rpc_sup_avg_lead_time_days_series,
    "rpc_sup_transport_cost_ratio_series": rpc_sup_transport_cost_ratio_series,
}
