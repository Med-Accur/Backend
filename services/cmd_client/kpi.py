from datetime import datetime

def parse_date_safe(value):
    """Convertit une date en objet datetime.date si possible, sinon retourne None."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return None


def get_kpi_nb_commandes(tables):
    commandes = tables.get("commandeclient", [])
    return sum(1 for c in commandes if c.get("statut") not in ("retournee", "Annulee"))


def get_kpi_taux_retards(tables):
    commandes = tables.get("commandeclient", [])
    total = len(commandes)
    retards = 0
    for c in commandes:
        date_reelle = parse_date_safe(c.get("date_reelle_livraison"))
        date_prevue = parse_date_safe(c.get("date_prevue_livraison"))
        if date_reelle and date_prevue and date_reelle > date_prevue:
            retards += 1
    return round(100.0 * retards / total, 2) if total > 0 else None


def get_kpi_otif(tables):
    commandes = tables.get("commandeclient", [])
    total = len(commandes)
    on_time = 0
    for c in commandes:
        date_reelle = parse_date_safe(c.get("date_reelle_livraison"))
        date_prevue = parse_date_safe(c.get("date_prevue_livraison"))
        if date_reelle and date_prevue and date_reelle <= date_prevue:
            on_time += 1
    return round(100.0 * on_time / total, 2) if total > 0 else None


def get_kpi_taux_annulation(tables):
    commandes = tables.get("commandeclient", [])
    total = len(commandes)
    annulees = sum(1 for c in commandes if c.get("statut") == "retournee")
    return round(100.0 * annulees / total, 2) if total > 0 else None


def get_kpi_duree_cycle_moyenne_jours(tables):
    commandes = tables.get("commandeclient", [])
    durees = []
    for c in commandes:
        date_commande = parse_date_safe(c.get("date_commande"))
        date_expedition = parse_date_safe(c.get("date_expedition"))
        if date_commande and date_expedition:
            durees.append((date_expedition - date_commande).days)
    return round(sum(durees) / len(durees), 2) if durees else None


def get_kpi_duree_moyenne_changelog(tables):
    changelogs = sorted(
        tables.get("changelog", []),
        key=lambda x: (x["commande_id"], x["date_changement_statut"])
    )
    durees = []
    for i in range(1, len(changelogs)):
        if changelogs[i]["commande_id"] == changelogs[i - 1]["commande_id"]:
            d1 = parse_date_safe(changelogs[i - 1]["date_changement_statut"])
            d2 = parse_date_safe(changelogs[i]["date_changement_statut"])
            if d1 and d2:
                durees.append((d2 - d1).days)
    return round(sum(durees) / len(durees), 2) if durees else None
