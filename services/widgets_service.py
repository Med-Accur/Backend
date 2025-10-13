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
from datetime import datetime, timedelta

def rpc_nb_otif(
    tables, 
    start_date: str = None, 
    end_date: str = None, 
    statut: str = None, 
    mode_livraison: str | None = None
):
    commandes = tables.get("commandeclient", [])
    modes_livraison = tables.get("modelivraison", [])

    today = datetime.today().date()
    if not end_date:
        end_date = today
    else:
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    if not start_date:
        start_date = end_date - timedelta(days=6)
    else:
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()

    # ---- Trouver l'ID correspondant au nom du mode de livraison ----
    mode_livraison_id = None
    if mode_livraison:
        for mode in modes_livraison:
            if mode["nom"].lower() == mode_livraison.lower():  # insensible à la casse
                mode_livraison_id = mode["id"]
                break

    # Générer la série de jours
    all_days = [(start_date + timedelta(days=i)) for i in range((end_date - start_date).days + 1)]
    stats_by_day = {day.strftime("%Y-%m-%d"): {"otif": 0, "commandes": 0} for day in all_days}

    for cmd in commandes:
        cmd_date = datetime.strptime(cmd["date_commande"], "%Y-%m-%d").date()
        if start_date <= cmd_date <= end_date:
            if statut and cmd.get("statut") != statut:
                continue
            if mode_livraison_id and cmd.get("mode_livraison_id") != mode_livraison_id:
                continue

            stats_by_day[cmd_date.strftime("%Y-%m-%d")]["commandes"] += 1

            date_prevue = cmd.get("date_prevue_livraison")
            date_reelle = cmd.get("date_reelle_livraison")

            if date_prevue and date_reelle:
                prevue = datetime.strptime(date_prevue, "%Y-%m-%d").date()
                reelle = datetime.strptime(date_reelle, "%Y-%m-%d").date()
                if reelle <= prevue:
                    stats_by_day[cmd_date.strftime("%Y-%m-%d")]["otif"] += 1

    # Construire la liste finale
    result = []
    for day in sorted(stats_by_day):
        result.append({
            "day": day,
            "otif": stats_by_day[day]["otif"],
            "commandes": stats_by_day[day]["commandes"]
        })

    return result


def rpc_taux_retard(
    tables, 
    start_date: str = None, 
    end_date: str = None, 
    statut: str = None, 
    mode_livraison: str | None = None
):
    commandes = tables.get("commandeclient", [])
    modes_livraison = tables.get("modelivraison", [])

    today = datetime.today().date()
    if not end_date:
        end_date = today
    else:
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    if not start_date:
        start_date = end_date - timedelta(days=6)
    else:
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()

    # ---- Trouver l'ID correspondant au nom du mode de livraison ----
    mode_livraison_id = None
    if mode_livraison:
        for mode in modes_livraison:
            if mode["nom"].lower() == mode_livraison.lower():  # insensible à la casse
                mode_livraison_id = mode["id"]
                break

    # Générer la série de jours
    all_days = [(start_date + timedelta(days=i)) for i in range((end_date - start_date).days + 1)]
    stats_by_day = {day.strftime("%Y-%m-%d"): {"retard": 0, "commandes": 0} for day in all_days}

    for cmd in commandes:
        cmd_date = datetime.strptime(cmd["date_commande"], "%Y-%m-%d").date()
        if start_date <= cmd_date <= end_date:
            if statut and cmd.get("statut") != statut:
                continue
            if mode_livraison_id and cmd.get("mode_livraison_id") != mode_livraison_id:
                continue

            stats_by_day[cmd_date.strftime("%Y-%m-%d")]["commandes"] += 1

            date_prevue = cmd.get("date_prevue_livraison")
            date_reelle = cmd.get("date_reelle_livraison")

            if date_prevue and date_reelle:
                prevue = datetime.strptime(date_prevue, "%Y-%m-%d").date()
                reelle = datetime.strptime(date_reelle, "%Y-%m-%d").date()
                if reelle > prevue:
                    stats_by_day[cmd_date.strftime("%Y-%m-%d")]["retard"] += 1

    # Construire la liste finale
    result = []
    for day in sorted(stats_by_day):
        commandes_day = stats_by_day[day]["commandes"]
        retard_day = stats_by_day[day]["retard"]
        result.append({
            "day": day,
            "retard": retard_day,
            "commandes": commandes_day
        })

    return result

def rpc_duree_cycle_moyenne(tables, start_date: str = None, end_date: str = None, mode_livraison_nom: str | None = None):
    commandes = tables.get("commandeclient", [])
    modes_livraison = tables.get("modelivraison", [])

    # ⚡ Valeurs par défaut : les 7 derniers jours
    today = datetime.today().date()
    if not end_date:
        end_date = today
    else:
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    if not start_date:
        start_date = end_date - timedelta(days=6)
    else:
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()

    # Trouver l'ID du mode de livraison si fourni
    mode_livraison_id = None
    if mode_livraison_nom:
        for mode in modes_livraison:
            if mode["nom"].lower() == mode_livraison_nom.lower():
                mode_livraison_id = mode["id"]
                break

    # Générer la série complète de dates
    all_days = [(start_date + timedelta(days=i)) for i in range((end_date - start_date).days + 1)]
    stats_by_day = {day.strftime("%Y-%m-%d"): {"durees": [], "commandes": 0} for day in all_days}

    # Parcourir les commandes
    for cmd in commandes:
        if not cmd.get("date_commande") or not cmd.get("date_expedition"):
            continue

        cmd_date = datetime.strptime(cmd["date_commande"], "%Y-%m-%d").date()
        if start_date <= cmd_date <= end_date:
            # Filtrer par mode livraison si nécessaire
            if mode_livraison_id and cmd.get("mode_livraison_id") != mode_livraison_id:
                continue

            expedition_date = datetime.strptime(cmd["date_expedition"], "%Y-%m-%d").date()
            duree = (expedition_date - cmd_date).days

            stats_by_day[cmd_date.strftime("%Y-%m-%d")]["durees"].append(duree)
            stats_by_day[cmd_date.strftime("%Y-%m-%d")]["commandes"] += 1

    # Construire la liste finale
    result = []
    for day in sorted(stats_by_day):
        durees = stats_by_day[day]["durees"]
        moyenne = round(sum(durees) / len(durees), 2) if durees else 0
        result.append({
            "day": day,
            "duree_moyenne": moyenne,
            "commandes": stats_by_day[day]["commandes"]
        })

    return result



def rpc_orders_chart(
    tables, 
    start_date: str = None, 
    end_date: str = None, 
    statut: str | None = None, 
    mode_livraison: str | None = None
):
    commandes = tables.get("commandeclient", [])
    modes_livraison = tables.get("modelivraison", [])

    # ⚡ Valeurs par défaut : les 7 derniers jours
    today = datetime.today().date()
    if not end_date:
        end_date = today
    else:
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    if not start_date:
        start_date = end_date - timedelta(days=6)
    else:
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()

    # ---- Trouver l'ID correspondant au nom du mode de livraison ----
    mode_livraison_id = None
    if mode_livraison:
        for mode in modes_livraison:
            if mode["nom"].lower() == mode_livraison.lower():  # insensible à la casse
                mode_livraison_id = mode["id"]
                break

    # ---- Filtrer commandes ----
    filtered = []
    for cmd in commandes:
        cmd_date = datetime.strptime(cmd["date_commande"], "%Y-%m-%d").date()
        if start_date <= cmd_date <= end_date:
            # Filtrer par statut si fourni
            if statut and cmd.get("statut") != statut:
                continue
            # Filtrer par mode_livraison si fourni
            if mode_livraison_id and cmd.get("mode_livraison_id") != mode_livraison_id:
                continue

            filtered.append(cmd)

    # ---- Générer la série complète de dates ----
    all_days = [(start_date + timedelta(days=i)) for i in range((end_date - start_date).days + 1)]
    counts_by_day = {day.strftime("%Y-%m-%d"): 0 for day in all_days}

    for cmd in filtered:
        day = cmd["date_commande"]
        counts_by_day[day] += 1

    orders_per_day = [{"day": day, "Commandes": counts_by_day[day]} for day in sorted(counts_by_day)]

    return orders_per_day

def rpc_taux_annulation(tables, start_date: str = None, end_date: str = None, mode_livraison: str | None = None):
    commandes = tables.get("commandeclient", [])
    modes_livraison = tables.get("modelivraison", [])

    # Dates par défaut : 7 derniers jours
    today = datetime.today().date()
    if not end_date:
        end_date = today
    else:
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    if not start_date:
        start_date = end_date - timedelta(days=6)
    else:
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()

    # Trouver l'ID correspondant au nom du mode de livraison
    mode_livraison_id = None
    if mode_livraison:
        for mode in modes_livraison:
            if mode["nom"].lower() == mode_livraison.lower():
                mode_livraison_id = mode["id"]
                break

    # Générer la série complète de dates
    all_days = [(start_date + timedelta(days=i)) for i in range((end_date - start_date).days + 1)]
    stats_by_day = {day.strftime("%Y-%m-%d"): {"annuler": 0, "commandes": 0} for day in all_days}

    # Parcourir les commandes
    for cmd in commandes:
        cmd_date = datetime.strptime(cmd["date_commande"], "%Y-%m-%d").date()
        if start_date <= cmd_date <= end_date:
            # Filtrer par mode livraison si fourni
            if mode_livraison_id and cmd.get("mode_livraison_id") != mode_livraison_id:
                continue

            stats_by_day[cmd_date.strftime("%Y-%m-%d")]["commandes"] += 1

            if cmd.get("statut") == "annulée":
                stats_by_day[cmd_date.strftime("%Y-%m-%d")]["annuler"] += 1

    # Construire la liste finale
    result = []
    for day in sorted(stats_by_day):
        result.append({
            "day": day,
            "annuler": stats_by_day[day]["annuler"],
            "commandes": stats_by_day[day]["commandes"]
        })

    return result


def rpc_duree_changelog_chart(tables, start_date: str = None, end_date: str = None):
    changelogs = tables.get("changelog", [])

    # ⚡ Valeurs par défaut (7 jours glissants)
    today = datetime.today().date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else today
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else end_date - timedelta(days=6)

    # Ordonner changelogs par commande et par date
    changelogs.sort(key=lambda x: (x["commande_id"], x["date_changement_statut"]))

    # Préparer les jours
    all_days = [(start_date + timedelta(days=i)) for i in range((end_date - start_date).days + 1)]
    stats_by_day = {day.strftime("%Y-%m-%d"): [] for day in all_days}

    # Calcul des durées entre statuts pour chaque commande
    for i in range(1, len(changelogs)):
        prev, curr = changelogs[i-1], changelogs[i]
        if prev["commande_id"] == curr["commande_id"]:
            d1 = datetime.strptime(prev["date_changement_statut"], "%Y-%m-%d").date()
            d2 = datetime.strptime(curr["date_changement_statut"], "%Y-%m-%d").date()
            if start_date <= d2 <= end_date:
                stats_by_day[d2.strftime("%Y-%m-%d")].append((d2 - d1).days)

    # Construire la série finale
    result = []
    for day in sorted(stats_by_day):
        durees = stats_by_day[day]
        result.append({
            "day": day,
            "duree_moyenne": round(sum(durees) / len(durees), 2) if durees else 0
        })

    return result



def get_table_cmd_clients_service(tables):
    commandes = tables.get("commandeclient", [])
    contacts = tables.get("contact", [])
    contact_map = {c["id"]: c["nom"] for c in contacts}
    result = []
    
    for c in commandes:
        client_name = contact_map.get(c["contact_id"], "Inconnu")
        
        date_reelle = c.get("date_reelle_livraison")
        date_prevue = c.get("date_prevue_livraison")

        if date_reelle and date_prevue:
            retard = "Oui" if date_reelle > date_prevue else "Non"
        else:
            retard = "Non"  # ou "Inconnu", selon ton besoin

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
    "rpc_duree_changelog_chart": rpc_duree_changelog_chart
    # Ajouter d’autres fonctions Python si besoin
}
