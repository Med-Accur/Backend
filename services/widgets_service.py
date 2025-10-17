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


from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional

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





    # services/stock/rpc.py





# ============================================================
# Helpers génériques (alignés avec cmd_client)
# ============================================================

def _parse_date(v: Any) -> Optional[date]:
    if v in (None, "", "null"): return None
    if isinstance(v, date): return v
    s = str(v)
    for fmt in ("%Y-%m-%d","%Y/%m/%d","%d/%m/%Y","%Y-%m-%d %H:%M:%S","%Y-%m-%dT%H:%M:%S"):
        try: return datetime.strptime(s[:len(fmt)], fmt).date()
        except: pass
    try: return datetime.fromisoformat(s).date()
    except: return None

def _between(d: Optional[date], a: date, b: date) -> bool:
    return bool(d and a <= d <= b)

def _resolve_window(start_date: Optional[str], end_date: Optional[str], default_days: int = 6) -> tuple[date, date]:
    today = date.today()
    end = _parse_date(end_date) or today
    start = _parse_date(start_date) or (end - timedelta(days=default_days))
    if start > end: start, end = end, start
    return start, end

def _days_range(start: date, end: date) -> List[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]

def _g(row: Dict[str, Any], *names: str, default=None):
    """Récupère la première colonne existante/non nulle parmi les alias donnés."""
    for n in names:
        if n in row and row[n] is not None:
            return row[n]
    return default

def _mvt_signed_quantity(typemvt: str, q: float) -> float:
    t = (typemvt or "").strip().lower()
    if t.startswith("entr") or t == "entrée":     # entrée
        return +q
    if t.startswith("sort"):                      # sortie
        return -q
    if t.startswith("ajust"):                     # ajustement: on garde le signe tel quel (peut être +/-)
        return q
    if t in ("perte", "shrink", "shrinkage"):     # pertes
        return -abs(q)
    return 0.0

# ============================================================
# Reconstruction du stock disponible quotidien
# ------------------------------------------------------------
# Principe "absolu" :
#   1) On prend le snapshot au end_date: sum(quantitedisponible - quantitereserve)
#   2) On recalcule le disponible pour chaque jour d en retirant la somme des mouvements de (d+1..end)
#      => dispo(d) = dispo(end) - suffix_sum_delta(d+1..end)
# C’est stable et donne une vraie valeur par jour même sans historiques journaliers.
# ============================================================

def _disponible_snapshot_end(tables: Dict[str, Any], entrepot_id: Any | None) -> float:
    stock_rows = tables.get("stock", []) or []
    total = 0.0
    for s in stock_rows:
        if entrepot_id is not None and s.get("entrepot_id") != entrepot_id:
            continue
        qd = float(s.get("quantitedisponible", 0) or 0)
        qr = float(s.get("quantitereserve", 0) or 0)
        total += max(0.0, qd - qr)
    return float(total)

def _aggregate_mvts_by_day(tables: Dict[str, Any], start: date, end: date, entrepot_id: Any | None) -> Dict[date, float]:
    mvts = tables.get("mouvement_stock", []) or []
    stock_rows = tables.get("stock", []) or []
    stock_ids_target = None
    if entrepot_id is not None:
        stock_ids_target = {s.get("id") for s in stock_rows if s.get("entrepot_id") == entrepot_id}

    by_day: Dict[date, float] = defaultdict(float)
    for m in mvts:
        dm = _parse_date(_g(m, "datemouvement", "date_mouvement"))
        if not _between(dm, start, end): 
            continue
        # filtre entrepôt (via entrepot_id direct ou via stock_id relié à cet entrepôt)
        if entrepot_id is not None:
            if m.get("entrepot_id") == entrepot_id:
                pass
            elif stock_ids_target and m.get("stock_id") in stock_ids_target:
                pass
            else:
                continue
        q = float(_g(m, "quantite", "qte", default=0) or 0)
        delta = _mvt_signed_quantity(str(m.get("typemouvement","")), q)
        by_day[dm] += delta
    return by_day

def _disponible_series_absolute(tables: Dict[str, Any], start: date, end: date, entrepot_id: Any | None) -> Dict[str, float]:
    days = _days_range(start, end)
    # delta mouvements par jour
    delta_by_day = _aggregate_mvts_by_day(tables, start, end, entrepot_id)
    # suffix sum Δ(d+1..end)
    suffix_after: Dict[date, float] = {}
    running = 0.0
    for d in reversed(days):
        suffix_after[d] = running
        running += delta_by_day.get(d, 0.0)

    dispo_end = _disponible_snapshot_end(tables, entrepot_id)
    out: Dict[str, float] = {}
    for d in days:
        val = dispo_end - suffix_after[d]
        out[d.strftime("%Y-%m-%d")] = round(float(val), 2)
    return out

# ============================================================
# 1) Stock disponible (série)
# ============================================================

def rpc_stock_disponible_series(tables, start_date: str | None = None, end_date: str | None = None, entrepot_id: Any | None = None):
    start, end = _resolve_window(start_date, end_date, default_days=6)
    dispo_by_day = _disponible_series_absolute(tables, start, end, entrepot_id)
    return [{"day": day, "Disponible": dispo_by_day[day]} for day in sorted(dispo_by_day)]

# ============================================================
# 2) Days On Hand (série)
# DOH(d) = Stock_dispo(d) / (Conso_moy_journalière sur [d-29..d])
# Conso = somme des sorties sur la fenêtre.
# ============================================================

def rpc_days_on_hand_series(tables, start_date: str | None = None, end_date: str | None = None, entrepot_id: Any | None = None):
    start, end = _resolve_window(start_date, end_date, default_days=6)
    days = _days_range(start, end)
    dispo_by_day = _disponible_series_absolute(tables, start, end, entrepot_id)

    mvts_by_day = _aggregate_mvts_by_day(tables, start - timedelta(days=29), end, entrepot_id)

    out = []
    for d in days:
        win_start = d - timedelta(days=29)
        # conso = sorties nettes (Δ négatifs) => on somme uniquement les -Δ
        conso = 0.0
        cur = win_start
        while cur <= d:
            delta = mvts_by_day.get(cur, 0.0)
            conso += max(0.0, -delta)
            cur += timedelta(days=1)
        doh = 0.0
        if conso > 0:
            avg = conso / 30.0
            doh = round(dispo_by_day[d.strftime("%Y-%m-%d")] / avg, 2) if avg > 0 else 0.0
        out.append({"day": d.strftime("%Y-%m-%d"), "DOH": doh})
    return out

# ============================================================
# 3) Taux de rotation (série)
# Rotation(d) = Coût des ventes 30j / Valeur_stock(d)
# Valeur_stock(d) ~ Valeur_stock(end) * (dispo(d)/dispo(end))  (approximation mix constant)
# Coût des ventes 30j ≈ Σ (quantite_commandee * prix_unitaire) sur commandes livrées [d-29..d]
# ============================================================

def _stock_value_end(tables: Dict[str, Any], entrepot_id: Any | None) -> float:
    produits = {p.get("id"): p for p in (tables.get("produit", []) or [])}
    stock_rows = tables.get("stock", []) or []
    total = 0.0
    for s in stock_rows:
        if entrepot_id is not None and s.get("entrepot_id") != entrepot_id:
            continue
        pid = s.get("produit_id")
        prix = float((produits.get(pid) or {}).get("prix", 0) or 0)
        q = float(s.get("quantitedisponible", 0) or 0)
        total += max(0.0, q) * max(0.0, prix)
    return float(total)

def _ca_30d(tables: Dict[str, Any], win_start: date, d: date) -> float:
    # calcule "coût des ventes" ou "CA" selon besoin — ici CA
    cmds = {c.get("id"): c for c in (tables.get("commandeclient", []) or [])}
    lignes = (tables.get("lignecommande", []) or [])
    total = 0.0
    for lc in lignes:
        cc = cmds.get(lc.get("commande_id"))
        dr = _parse_date(_g(cc or {}, "date_reelle_livraison"))
        if not _between(dr, win_start, d):
            continue
        q = float(_g(lc, "quantite_commandee", "quantite", default=0) or 0)
        pu = float(_g(lc, "prix_unitaire", "prix", default=0) or 0)
        total += max(0.0, q * pu)
    return float(total)

def rpc_taux_rotation_series(tables, start_date: str | None = None, end_date: str | None = None, entrepot_id: Any | None = None):
    start, end = _resolve_window(start_date, end_date, default_days=6)
    days = _days_range(start, end)

    dispo_by_day = _disponible_series_absolute(tables, start, end, entrepot_id)
    dispo_end = max(0.0001, sum([0.0]) + dispo_by_day.get(end.strftime("%Y-%m-%d"), 0.0))  # évite /0
    stock_val_end = _stock_value_end(tables, entrepot_id)

    out = []
    for d in days:
        win_start = d - timedelta(days=29)
        ca = _ca_30d(tables, win_start, d)
        # valeur stock d (approx. proportionnelle au dispo)
        dispo_d = dispo_by_day[d.strftime("%Y-%m-%d")]
        stock_val_d = (stock_val_end * dispo_d / dispo_end) if dispo_end > 0 else 0.0
        rot = round((ca / stock_val_d), 2) if stock_val_d > 0 else 0.0
        out.append({"day": d.strftime("%Y-%m-%d"), "Rotation": rot})
    return out

# ============================================================
# 4) Inventory to Sales (série)
# I2S(d) = Valeur_stock(d) / CA_30j(d)
# ============================================================

def rpc_inventory_to_sales_series(tables, start_date: str | None = None, end_date: str | None = None, entrepot_id: Any | None = None):
    start, end = _resolve_window(start_date, end_date, default_days=6)
    days = _days_range(start, end)

    dispo_by_day = _disponible_series_absolute(tables, start, end, entrepot_id)
    dispo_end = max(0.0001, dispo_by_day.get(end.strftime("%Y-%m-%d"), 0.0))
    stock_val_end = _stock_value_end(tables, entrepot_id)

    out = []
    for d in days:
        win_start = d - timedelta(days=29)
        ca = _ca_30d(tables, win_start, d)
        dispo_d = dispo_by_day[d.strftime("%Y-%m-%d")]
        stock_val_d = (stock_val_end * dispo_d / dispo_end) if dispo_end > 0 else 0.0
        i2s = round(stock_val_d / ca, 2) if ca > 0 else 0.0
        out.append({"day": d.strftime("%Y-%m-%d"), "I2S": i2s})
    return out

# ============================================================
# 5) Rentabilité du stock (série)
# Rentab(d) = Marge_30j(d) / Valeur_stock(d)
# Marge_30j = CA_30j - Coût_des_ventes(=CA_30j*cout_ratio) - couts_logistiques
# ============================================================

def rpc_rentabilite_stock_series(
    tables,
    start_date: str | None = None,
    end_date: str | None = None,
    entrepot_id: Any | None = None,
    cout_ratio: float = 0.7,
    couts_logistiques: float = 0.0
):
    start, end = _resolve_window(start_date, end_date, default_days=6)
    days = _days_range(start, end)

    dispo_by_day = _disponible_series_absolute(tables, start, end, entrepot_id)
    dispo_end = max(0.0001, dispo_by_day.get(end.strftime("%Y-%m-%d"), 0.0))
    stock_val_end = _stock_value_end(tables, entrepot_id)

    out = []
    for d in days:
        win_start = d - timedelta(days=29)
        ca = _ca_30d(tables, win_start, d)
        cdv = ca * max(0.0, min(1.0, cout_ratio))
        marge = ca - cdv - max(0.0, couts_logistiques)

        dispo_d = dispo_by_day[d.strftime("%Y-%m-%d")]
        stock_val_d = (stock_val_end * dispo_d / dispo_end) if dispo_end > 0 else 0.0

        rent = round(marge / stock_val_d, 2) if stock_val_d > 0 else 0.0
        out.append({"day": d.strftime("%Y-%m-%d"), "Rentabilité": rent})
    return out

# ============================================================
# 6) Taux de rupture (série)
# Rupture%(d) = (Demande_non_servie_30j / Demande_totale_30j) * 100
# Demande 30j = somme des quantités commandées sur lignes livrées [d-29..d]
# Servi 30j   = somme des mouvements "sortie" [d-29..d] (par produit)
# ============================================================

def rpc_taux_rupture_series(tables, start_date: str | None = None, end_date: str | None = None, entrepot_id: Any | None = None):
    start, end = _resolve_window(start_date, end_date, default_days=6)
    days = _days_range(start, end)

    cmds = {c.get("id"): c for c in (tables.get("commandeclient", []) or [])}
    lignes = (tables.get("lignecommande", []) or [])
    stock_rows = tables.get("stock", []) or []

    # mapping stock_id -> produit_id (pour mvts sans produit_id)
    stock_prod_map = { s.get("id"): s.get("produit_id") for s in stock_rows }

    # Pré-agrégation des mouvements par jour/produit (sorties)
    mvts = tables.get("mouvement_stock", []) or []
    if entrepot_id is not None:
        stock_ids_target = {s.get("id") for s in stock_rows if s.get("entrepot_id") == entrepot_id}
    else:
        stock_ids_target = None

    sorties_by_day_prod: Dict[date, Dict[Any, float]] = defaultdict(lambda: defaultdict(float))
    for m in mvts:
        typ = str(m.get("typemouvement","")).lower()
        if not typ.startswith("sortie"): 
            continue
        dm = _parse_date(_g(m, "datemouvement", "date_mouvement"))
        if dm is None: 
            continue
        if entrepot_id is not None:
            if m.get("entrepot_id") == entrepot_id:
                pass
            elif stock_ids_target and m.get("stock_id") in stock_ids_target:
                pass
            else:
                continue
        pid = m.get("produit_id") or stock_prod_map.get(m.get("stock_id"))
        if pid is None: 
            continue
        q = float(_g(m, "quantite", "qte", default=0) or 0)
        sorties_by_day_prod[dm][pid] += max(0.0, q)

    out = []
    for d in days:
        win_start = d - timedelta(days=29)
        # Demande par produit (lignes livrées)
        dem: Dict[Any, float] = defaultdict(float)
        for lc in lignes:
            cc = cmds.get(lc.get("commande_id"))
            dr = _parse_date(_g(cc or {}, "date_reelle_livraison"))
            if not _between(dr, win_start, d): 
                continue
            pid = lc.get("produit_id")
            dem[pid] += max(0.0, float(_g(lc, "quantite_commandee", "quantite", default=0) or 0))
        total_dem = sum(dem.values())

        # Servi par produit via mvts "sortie" sur la fenêtre
        serv: Dict[Any, float] = defaultdict(float)
        cur = win_start
        while cur <= d:
            for pid, q in sorties_by_day_prod.get(cur, {}).items():
                serv[pid] += q
            cur += timedelta(days=1)

        non_serv = 0.0
        for pid, qd in dem.items():
            qs = serv.get(pid, 0.0)
            non_serv += max(0.0, qd - min(qd, qs))

        taux = round(100.0 * non_serv / total_dem, 2) if total_dem > 0 else 0.0
        out.append({"day": d.strftime("%Y-%m-%d"), "Rupture %": taux})
    return out

# ============================================================
# 7) Remaining shelf life (moyenne pondérée en jours) — série
# Pour chaque jour d: moyenne( max(0, (exp - d).days) pondérée par quantités )
# => diminue de ~1 par jour si les données de péremption sont fixes.
# ============================================================

def rpc_remaining_shelf_life_series(tables, start_date: str | None = None, end_date: str | None = None, entrepot_id: Any | None = None):
    start, end = _resolve_window(start_date, end_date, default_days=6)
    days = _days_range(start, end)

    per = tables.get("peremption", []) or []
    stock_rows = tables.get("stock", []) or []
    if entrepot_id is not None:
        stock_ids_target = {s.get("id") for s in stock_rows if s.get("entrepot_id") == entrepot_id}
    else:
        stock_ids_target = None

    out = []
    for d in days:
        tot_q = tot_j = 0.0
        for r in per:
            # filtre entrepôt si possible via stock_id
            if stock_ids_target is not None:
                sid = r.get("stock_id")
                if sid not in stock_ids_target:
                    continue
            exp = _parse_date(_g(r, "dateexpiration", "date_expiration"))
            if not exp: 
                continue
            q = float(_g(r, "quantite", "qte", default=0) or 0)
            j = max(0, (exp - d).days)
            tot_q += q; tot_j += q * j
        avg = round(tot_j / tot_q, 1) if tot_q > 0 else 0.0
        out.append({"day": d.strftime("%Y-%m-%d"), "Avg days": avg})
    return out

# ============================================================
# 8) Contraction (shrinkage) par jour — série
# Somme des pertes et ajustements négatifs par jour sur la fenêtre.
# ============================================================

def rpc_shrinkage_by_day(tables, start_date: str | None = None, end_date: str | None = None, entrepot_id: Any | None = None):
    start, end = _resolve_window(start_date, end_date, default_days=6)
    days = _days_range(start, end)

    mvts = tables.get("mouvement_stock", []) or []
    stock_rows = tables.get("stock", []) or []
    if entrepot_id is not None:
        stock_ids_target = {s.get("id") for s in stock_rows if s.get("entrepot_id") == entrepot_id}
    else:
        stock_ids_target = None

    agg = { d.strftime("%Y-%m-%d"): 0.0 for d in days }
    for m in mvts:
        dm = _parse_date(_g(m, "datemouvement", "date_mouvement"))
        if not _between(dm, start, end): 
            continue
        if entrepot_id is not None:
            if m.get("entrepot_id") == entrepot_id:
                pass
            elif stock_ids_target and m.get("stock_id") in stock_ids_target:
                pass
            else:
                continue
        t = str(m.get("typemouvement","")).strip().lower()
        q = float(_g(m, "quantite", "qte", default=0) or 0)
        # on ne garde QUE les pertes / ajustements négatifs
        if t in ("perte","shrink","shrinkage"):
            val = abs(q)
        elif t.startswith("ajustement") and q < 0:
            val = abs(q)
        else:
            val = 0.0
        k = dm.strftime("%Y-%m-%d")
        agg[k] = round(agg.get(k, 0.0) + val, 2)

    return [{"day": day, "Quantité": agg[day]} for day in sorted(agg)]


## services/fournisseur/rpc_series.py


# 

def _parse_date(v: Any) -> Optional[date]:
    if v in (None, "", "null"): 
        return None
    if isinstance(v, date):
        return v
    s = str(v)
    for fmt in ("%Y-%m-%d","%Y/%m/%d","%d/%m/%Y","%Y-%m-%d %H:%M:%S","%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s[:len(fmt)], fmt).date()
        except:
            pass
    try:
        return datetime.fromisoformat(s).date()
    except:
        return None

def _between(d: Optional[date], a: date, b: date) -> bool:
    return bool(d and a <= d <= b)

def _days_range(start: date, end: date) -> List[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]

def _g(row: Dict[str, Any], *names: str, default=None):
    """Récupère la première clé existante (gère tes variantes sans/avec underscores)."""
    for n in names:
        if n in row and row[n] is not None:
            return row[n]
    return default

# ---- Fenêtre auto basée sur les données fournisseur (si start/end absents) ----
def _latest_sup_date(tables: Dict[str, Any]) -> Optional[date]:
    pool: List[date] = []

    for c in (tables.get("commande_fournisseur", []) or []):
        for k in ("datereellelivraison","dateprevuelivraison","datecommande",
                  "date_reelle_livraison","date_prevue_livraison","date_commande"):
            pool.append(_parse_date(c.get(k)))

    for r in (tables.get("reception_fournisseur", []) or []):
        for k in ("datereception","datevalidation","date_reception","date_reelle_livraison"):
            pool.append(_parse_date(r.get(k)))

    for ret in (tables.get("retour_fournisseur", []) or []):
        for k in ("dateretour","date_retour"):
            pool.append(_parse_date(ret.get(k)))

    pool = [d for d in pool if d]
    return max(pool) if pool else None

def _resolve_window_sup(tables: Dict[str, Any], 
                        start_date: Optional[str], 
                        end_date: Optional[str], 
                        default_days: int = 6) -> tuple[date, date]:
    """Si pas de dates fournies -> fin = dernière date dispo dans les tables fournisseurs (sinon today)."""
    last = _latest_sup_date(tables) or date.today()
    end = _parse_date(end_date) or last
    start = _parse_date(start_date) or (end - timedelta(days=default_days))
    if start > end:
        start, end = end, start
    return start, end


# ===============  SÉRIES KPI FOURNISSEUR (par jour)  ==================


# 1) % livraisons à l'heure (<= date prévue)
# -------- On-Time rate (livré à l'heure %) — robuste --------
def rpc_sup_on_time_rate_series(tables, start_date: str | None = None, end_date: str | None = None):
    start, end = _resolve_window(start_date, end_date, 6)
    days = _days_range(start, end)

    recs = tables.get("reception_fournisseur", []) or []
    cmds_by_id = {c.get("id"): c for c in (tables.get("commande_fournisseur", []) or [])}

    def _planned_date(rec, cmd):
        """
        Cherche une date prévue sur la réception PUIS sur la commande.
        Ajoute ici tous les alias possibles chez toi.
        """
        # Essayer côté réception
        for k in ("date_prevue_livraison", "date_prevue", "date_prevue_reception"):
            d = _parse_date((rec or {}).get(k))
            if d: return d
        # Puis côté commande
        for k in ("date_prevue_livraison", "date_prevue"):
            d = _parse_date((cmd or {}).get(k))
            if d: return d

        # OPTIONAL fallback: date_commande + delai_prevu_jours
        # if cmd and cmd.get("delai_prevu_jours"):
        #     dc = _parse_date(cmd.get("date_commande") or cmd.get("datecommande"))
        #     if dc:
        #         try:
        #             return dc + timedelta(days=int(cmd.get("delai_prevu_jours") or 0))
        #         except:
        #             pass
        return None

    def _actual_date(rec, cmd):
        """
        Date réelle de livraison: privilégier la réception, sinon commande.
        """
        for k in ("date_reception", "date_reelle_livraison"):
            d = _parse_date((rec or {}).get(k))
            if d: return d
        for k in ("date_reelle_livraison",):
            d = _parse_date((cmd or {}).get(k))
            if d: return d
        return None

    out = []
    for d in days:
        win_start = d - timedelta(days=29)
        total = on_time = 0

        for r in recs:
            cmd = cmds_by_id.get(
                _g(r, "commande_id", "commandefournisseur_id", "id_commandefournisseur", "commande_fournisseur_id")
            )

            planned = _planned_date(r, cmd)
            actual  = _actual_date(r, cmd)

            if not (planned and actual):
                continue  # sans date prévue et réelle on ne peut rien conclure

            if not _between(actual, win_start, d):
                continue  # fenêtre glissante 30j

            total += 1
            if actual <= planned:
                on_time += 1

        val = round(100.0 * on_time / total, 2) if total > 0 else 0.0
        out.append({"day": d.strftime("%Y-%m-%d"), "On-Time %": val})

    return out

# 2) % réceptions conformes
def rpc_sup_quality_conform_rate_series(tables, start_date: str | None = None, end_date: str | None = None):
    start, end = _resolve_window_sup(tables, start_date, end_date, 6)
    days = _days_range(start, end)
    recs = tables.get("reception_fournisseur", []) or []

    out = []
    for d in days:
        win_start = d - timedelta(days=29)
        total = ok = 0
        for r in recs:
            dr = _parse_date(_g(r, "date_reception","date_reelle_livraison","datereception","datevalidation"))
            if not _between(dr, win_start, d): 
                continue
            statut = str(_g(r, "statut_conformite","statutconformite","")).lower()
            if not statut:
                continue
            total += 1
            if statut in ("conforme","ok","valide"):
                ok += 1
        val = round(100.0 * ok / total, 2) if total > 0 else 0.0
        out.append({"day": d.strftime("%Y-%m-%d"), "Conformité %": val})
    return out

# 3) (Qté non conforme / Qté totale reçue) * 100
def rpc_sup_quality_nonconform_rate_series(tables, start_date: str | None = None, end_date: str | None = None):
    start, end = _resolve_window_sup(tables, start_date, end_date, 6)
    days = _days_range(start, end)
    lignes = tables.get("ligne_cmd_fournisseur", []) or []
    cmds_by_id = {c.get("id"): c for c in (tables.get("commande_fournisseur", []) or [])}

    out = []
    for d in days:
        win_start = d - timedelta(days=29)
        q_total = q_nc = 0.0
        for l in lignes:
            cmd = cmds_by_id.get(_g(l, "commande_id","commandefournisseur_id","id_commandefournisseur"))
            dref = (
                _parse_date(_g(cmd or {}, "date_reelle_livraison","datereellelivraison")) 
                or _parse_date(_g(l, "date_reception","datereception","dateprevisionnelle"))
            )
            if not _between(dref, win_start, d): 
                continue
            qrec = float(_g(l, "quantite_recue","quantiterecue", default=0) or 0)
            statut = str(_g(l, "statut_conformite","statutconformite","")).lower()
            q_total += max(0.0, qrec)
            if statut in ("nonconforme","non_conforme","rejet","defaut"):
                q_nc += max(0.0, qrec)
        val = round(100.0 * q_nc / q_total, 2) if q_total > 0 else 0.0
        out.append({"day": d.strftime("%Y-%m-%d"), "Non-conformité %": val})
    return out

# 4) nb retours / nb réceptions
def rpc_sup_return_rate_series(tables, start_date: str | None = None, end_date: str | None = None):
    start, end = _resolve_window_sup(tables, start_date, end_date, 6)
    days = _days_range(start, end)
    retours = tables.get("retour_fournisseur", []) or []
    recs    = tables.get("reception_fournisseur", []) or []

    out = []
    for d in days:
        win_start = d - timedelta(days=29)
        nb_ret = sum(1 for r in retours if _between(_parse_date(_g(r, "date_retour","dateretour")), win_start, d))
        nb_rec = sum(1 for r in recs    if _between(_parse_date(_g(r, "date_reception","date_reelle_livraison","datereception","datevalidation")), win_start, d))
        val = round(100.0 * nb_ret / nb_rec, 2) if nb_rec > 0 else 0.0
        out.append({"day": d.strftime("%Y-%m-%d"), "Retours %": val})
    return out

# 5) Lead time moyen (jours)
def rpc_sup_avg_lead_time_days_series(tables, start_date: str | None = None, end_date: str | None = None):
    start, end = _resolve_window_sup(tables, start_date, end_date, 6)
    days = _days_range(start, end)
    cmds = tables.get("commande_fournisseur", []) or []
    recs = tables.get("reception_fournisseur", []) or []
    recs_by_cmd: Dict[Any, List[date]] = {}
    for r in recs:
        cid = _g(r, "commande_id","commandefournisseur_id","id_commandefournisseur")
        drec = _parse_date(_g(r, "date_reception","date_reelle_livraison","datereception","datevalidation"))
        if cid is not None and drec:
            recs_by_cmd.setdefault(cid, []).append(drec)

    out = []
    for d in days:
        win_start = d - timedelta(days=29)
        delais: List[int] = []
        for c in cmds:
            dcmd = _parse_date(_g(c, "date_commande","datecommande"))
            drec = min(recs_by_cmd.get(c.get("id"), []), default=None) or _parse_date(_g(c, "date_reelle_livraison","datereellelivraison"))
            if not (dcmd and drec):
                continue
            if not _between(drec, win_start, d):
                continue
            delais.append((drec - dcmd).days)
        val = round(sum(delais)/len(delais), 2) if delais else 0.0
        out.append({"day": d.strftime("%Y-%m-%d"), "Lead time (j)": val})
    return out

# 6) Coût transport / montant commande
def rpc_sup_transport_cost_ratio_series(tables, start_date: str | None = None, end_date: str | None = None):
    start, end = _resolve_window_sup(tables, start_date, end_date, 6)
    days = _days_range(start, end)
    cmds = tables.get("commande_fournisseur", []) or []

    out = []
    for d in days:
        win_start = d - timedelta(days=29)
        cost = base = 0.0
        for c in cmds:
            dref = _parse_date(_g(c, "date_reelle_livraison","datereellelivraison")) or _parse_date(_g(c, "date_commande","datecommande"))
            if not _between(dref, win_start, d):
                continue
            cost += float(_g(c, "cout_transport","couttransport", default=0) or 0)
            base += float(_g(c, "montant_commande","montantcommande", default=0) or 0)
        val = round(cost / base, 4) if base > 0 else 0.0
        out.append({"day": d.strftime("%Y-%m-%d"), "Transport/Commande": val})
    return out




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
