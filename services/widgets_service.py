# services/widgets_service.py
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
    get_kpi_nb_commandes,
    get_kpi_taux_retards,
    get_kpi_otif,
    get_kpi_taux_annulation,
    get_kpi_duree_cycle_moyenne_jours
)

def get_table_cmd_clients_service(tables):
    commandes = tables.get("commandeclient", [])
    contacts = tables.get("contact", [])
    contact_map = {c["id"]: c["nom"] for c in contacts}
    result = []
    for c in commandes:
        client_name = contact_map.get(c["contact_id"], "Inconnu")
        retard = "Oui" if c.get("date_reelle_livraison") > c.get("date_prevue_livraison") else "Non"
        result.append({
            "numero_commande": c["id"],
            "client": client_name,
            "statut": c["statut"],
            "date_commande": c["date_commande"],
            "retard": retard
        })
    return result


def get_change_log(tables):
    changelog = tables.get("changelog", [])

    # Grouper par commande_id
    changelog_by_cmd = defaultdict(list)
    for c in changelog:
        changelog_by_cmd[c["commande_id"]].append(c)

    result = []
    for cmd_id, changes in changelog_by_cmd.items():
        changes_sorted = changes
        ancien_statut = 'first'
        for change in changes_sorted:
            result.append({
                "commande_id": cmd_id,
                "ancien_statut": ancien_statut,
                "nouveau_statut": change["statut"],
                "date_changement_statut": change["date_changement_statut"]
            })
            ancien_statut = change["statut"]
    return result


RPC_PYTHON_MAP = {
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
    # Ajouter d’autres fonctions Python si besoin
}
