# services/widgets_service.py
from collections import defaultdict
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
        # Trier par date_changement_statut pour simuler LAG
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


# Mapping dynamique RPC → fonction Python
RPC_PYTHON_MAP = {
    "get_table_cmd_clients": get_table_cmd_clients_service,
    "get_change_log": get_change_log,
    "kpi_nb_commandes": get_kpi_nb_commandes,
    "kpi_taux_retards": get_kpi_taux_retards,
    "kpi_otif": get_kpi_otif,
    "kpi_taux_annulation": get_kpi_taux_annulation,
    "kpi_duree_cycle_moyenne_jours": get_kpi_duree_cycle_moyenne_jours,
    # Ajouter d’autres fonctions Python si besoin
}
