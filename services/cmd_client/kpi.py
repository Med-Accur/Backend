from datetime import datetime

def get_kpi_nb_commandes(tables):
    commandes = tables.get("commandeclient", [])
    return sum(1 for c in commandes if c["statut"] not in ("retournee", "Annulee"))

def get_kpi_taux_retards(tables):
    commandes = tables.get("commandeclient", [])
    total = len(commandes)
    retards = sum(1 for c in commandes if c.get("date_reelle_livraison") > c.get("date_prevue_livraison"))
    return round(100.0 * retards / total, 2) if total > 0 else None

def get_kpi_otif(tables):
    commandes = tables.get("commandeclient", [])
    total = len(commandes)
    on_time = sum(1 for c in commandes if c.get("date_reelle_livraison") and c.get("date_prevue_livraison") and c["date_reelle_livraison"] <= c["date_prevue_livraison"])
    return round(100.0 * on_time / total, 2) if total > 0 else None

def get_kpi_taux_annulation(tables):
    commandes = tables.get("commandeclient", [])
    total = len(commandes)
    annulees = sum(1 for c in commandes if c["statut"] == "retournee")
    return round(100.0 * annulees / total, 2) if total > 0 else None

def get_kpi_duree_cycle_moyenne_jours(tables):
    commandes = tables.get("commandeclient", [])
    durees = [(datetime.strptime(c["date_expedition"], "%Y-%m-%d").date() - datetime.strptime(c["date_commande"], "%Y-%m-%d").date()).days for c in commandes if c.get("date_reelle_livraison") and c.get("date_commande")]
    return round(sum(durees)/len(durees), 2) if durees else None

from datetime import datetime

def get_kpi_duree_moyenne_changelog(tables):
    changelogs = sorted(tables.get("changelog", []), key=lambda x: (x["commande_id"], x["date_changement_statut"]))
    durees = [
        (datetime.strptime(changelogs[i]["date_changement_statut"], "%Y-%m-%d").date() -
         datetime.strptime(changelogs[i-1]["date_changement_statut"], "%Y-%m-%d").date()).days
        for i in range(1, len(changelogs))
        if changelogs[i]["commande_id"] == changelogs[i-1]["commande_id"]
    ]
    return round(sum(durees) / len(durees), 2) if durees else None
