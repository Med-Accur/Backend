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


# ---------- helpers communs (même style que cmd_client) ----------
def _parse_date(v: Any) -> Optional[date]:
    if v is None or v == "": return None
    if isinstance(v, date):  return v
    s = str(v)
    for fmt in ("%Y-%m-%d","%Y/%m/%d","%d/%m/%Y","%Y-%m-%d %H:%M:%S","%Y-%m-%dT%H:%M:%S"):
        try: return datetime.strptime(s[:len(fmt)], fmt).date()
        except: pass
    try: return datetime.fromisoformat(s).date()
    except: return None

def _between(d: Optional[date], a: date, b: date) -> bool:
    return bool(d and a <= d <= b)

def _resolve_window(start_date: Optional[str], end_date: Optional[str], default_days: int = 6) -> tuple[date,date]:
    today = date.today()
    end = _parse_date(end_date) or today
    start = _parse_date(start_date) or (end - timedelta(days=default_days))
    if start > end: start, end = end, start
    return start, end

def _days_range(start: date, end: date) -> List[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]

def _stock_by_entrepot(stock_rows: List[Dict[str, Any]], entrepot_id: Any | None) -> List[Dict[str, Any]]:
    if not entrepot_id: return stock_rows or []
    return [s for s in (stock_rows or []) if s.get("entrepot_id") == entrepot_id]

def _mvts_by_entrepot(mvts: List[Dict[str, Any]], stock_rows: List[Dict[str, Any]], entrepot_id: Any | None) -> List[Dict[str, Any]]:
    if not entrepot_id: return mvts or []
    stock_map_entrepot = { s.get("id"): s.get("entrepot_id") for s in (stock_rows or []) }
    out = []
    for m in (mvts or []):
        e_id = m.get("entrepot_id")
        if e_id is None and m.get("stock_id") in stock_map_entrepot:
            e_id = stock_map_entrepot.get(m.get("stock_id"))
        if e_id == entrepot_id:
            out.append(m)
    return out

def _stock_disponible_snapshot(tables: Dict[str, Any], entrepot_id: Any | None = None) -> float:
    stock_rows = _stock_by_entrepot(tables.get("stock", []) or [], entrepot_id)
    return float(sum(max(0.0, (float(s.get("quantitedisponible", 0) or 0) - float(s.get("quantitereserve", 0) or 0))) for s in stock_rows))

def _valeur_stock_courante(tables: Dict[str, Any], entrepot_id: Any | None = None) -> float:
    produits = {p.get("id"): p for p in (tables.get("produit", []) or [])}
    stock_rows = _stock_by_entrepot(tables.get("stock", []) or [], entrepot_id)
    total = 0.0
    for s in stock_rows:
        prix = float((produits.get(s.get("produit_id")) or {}).get("prix", 0) or 0)
        total += max(0.0, float(s.get("quantitedisponible", 0) or 0)) * max(0.0, prix)
    return float(total)

# ---------- 1) stock disponible (series) ----------
def rpc_stock_disponible_series(tables, start_date: str | None = None, end_date: str | None = None, entrepot_id: Any | None = None):
    start, end = _resolve_window(start_date, end_date, default_days=6)
    days = _days_range(start, end)
    val = _stock_disponible_snapshot(tables, entrepot_id)   # snapshot actuel (faute d'historique)
    return [{"day": d.strftime("%Y-%m-%d"), "Disponible": float(val)} for d in days]

# ---------- 2) days on hand (series) ----------
def rpc_days_on_hand_series(tables, start_date: str | None = None, end_date: str | None = None, entrepot_id: Any | None = None):
    start, end = _resolve_window(start_date, end_date, default_days=6)
    days = _days_range(start, end)
    mvts_all = tables.get("mouvement_stock", []) or []
    stock_rows = tables.get("stock", []) or []
    mvts = _mvts_by_entrepot(mvts_all, stock_rows, entrepot_id)
    dispo_now = _stock_disponible_snapshot(tables, entrepot_id)

    out = []
    for d in days:
        win_start = d - timedelta(days=29)
        conso = 0.0
        for m in mvts:
            if str(m.get("typemouvement","")).lower() != "sortie": continue
            dm = _parse_date(m.get("datemouvement"))
            if _between(dm, win_start, d):
                conso += max(0.0, float(m.get("quantite", 0) or 0))
        doh = 0
        if conso > 0:
            avg = conso / 30.0
            doh = round(dispo_now / avg, 2) if avg > 0 else 0
        out.append({"day": d.strftime("%Y-%m-%d"), "DOH": doh})
    return out

# ---------- 3) taux rotation (series) ----------
def rpc_taux_rotation_series(tables, start_date: str | None = None, end_date: str | None = None, entrepot_id: Any | None = None):
    start, end = _resolve_window(start_date, end_date, default_days=6)
    days = _days_range(start, end)
    cmds = {c.get("id"): c for c in (tables.get("commandeclient", []) or [])}
    lignes = (tables.get("lignecommande", []) or [])
    stock_val = _valeur_stock_courante(tables, entrepot_id=None)  # snapshot

    out = []
    for d in days:
        win_start = d - timedelta(days=29)
        cout_ventes = 0.0
        for lc in lignes:
            cc = cmds.get(lc.get("commande_id"))
            dr = _parse_date(cc.get("date_reelle_livraison")) if cc else None
            if _between(dr, win_start, d):
                q = float(lc.get("quantite_commandee", 0) or 0)
                pu = float(lc.get("prix_unitaire", 0) or 0)
                cout_ventes += max(0.0, q * pu)
        rot = round(cout_ventes / stock_val, 2) if stock_val > 0 else 0
        out.append({"day": d.strftime("%Y-%m-%d"), "Rotation": rot})
    return out

# ---------- 4) inventory to sales (series) ----------
def rpc_inventory_to_sales_series(tables, start_date: str | None = None, end_date: str | None = None, entrepot_id: Any | None = None):
    start, end = _resolve_window(start_date, end_date, default_days=6)
    days = _days_range(start, end)
    cmds = {c.get("id"): c for c in (tables.get("commandeclient", []) or [])}
    lignes = (tables.get("lignecommande", []) or [])
    stock_val = _valeur_stock_courante(tables, entrepot_id=None)  # snapshot

    out = []
    for d in days:
        win_start = d - timedelta(days=29)
        ca = 0.0
        for lc in lignes:
            cc = cmds.get(lc.get("commande_id"))
            dr = _parse_date(cc.get("date_reelle_livraison")) if cc else None
            if _between(dr, win_start, d):
                ca += max(0.0, float(lc.get("quantite_commandee", 0) or 0) * float(lc.get("prix_unitaire", 0) or 0))
        i2s = round(stock_val / ca, 2) if ca > 0 else 0
        out.append({"day": d.strftime("%Y-%m-%d"), "I2S": i2s})
    return out

# ---------- 5) rentabilite du stock (series) ----------
def rpc_rentabilite_stock_series(tables, start_date: str | None = None, end_date: str | None = None, entrepot_id: Any | None = None, cout_ratio: float = 0.7, couts_logistiques: float = 0.0):
    start, end = _resolve_window(start_date, end_date, default_days=6)
    days = _days_range(start, end)
    cmds = {c.get("id"): c for c in (tables.get("commandeclient", []) or [])}
    lignes = (tables.get("lignecommande", []) or [])
    stock_val = _valeur_stock_courante(tables, entrepot_id=None)  # snapshot

    out = []
    for d in days:
        win_start = d - timedelta(days=29)
        ca = 0.0
        for lc in lignes:
            cc = cmds.get(lc.get("commande_id"))
            dr = _parse_date(cc.get("date_reelle_livraison")) if cc else None
            if _between(dr, win_start, d):
                ca += max(0.0, float(lc.get("quantite_commandee", 0) or 0) * float(lc.get("prix_unitaire", 0) or 0))
        cdv = ca * max(0.0, min(1.0, cout_ratio))
        marge = ca - cdv - max(0.0, couts_logistiques)
        rent = round(marge / stock_val, 2) if stock_val > 0 else 0
        out.append({"day": d.strftime("%Y-%m-%d"), "Rentabilité": rent})
    return out

# ---------- 6) taux rupture (series) ----------
def rpc_taux_rupture_series(tables, start_date: str | None = None, end_date: str | None = None, entrepot_id: Any | None = None):
    start, end = _resolve_window(start_date, end_date, default_days=6)
    days = _days_range(start, end)

    cmds = {c.get("id"): c for c in (tables.get("commandeclient", []) or [])}
    lignes = (tables.get("lignecommande", []) or [])
    mvts_all = tables.get("mouvement_stock", []) or []
    stock_rows = tables.get("stock", []) or []
    mvts = _mvts_by_entrepot(mvts_all, stock_rows, entrepot_id)
    stock_prod_map = { s.get("id"): s.get("produit_id") for s in stock_rows }

    out = []
    for d in days:
        win_start = d - timedelta(days=29)
        # demande
        dem: Dict[Any, float] = {}
        for lc in lignes:
            cc = cmds.get(lc.get("commande_id"))
            dr = _parse_date(cc.get("date_reelle_livraison")) if cc else None
            if not _between(dr, win_start, d): continue
            pid = lc.get("produit_id")
            dem[pid] = dem.get(pid, 0.0) + max(0.0, float(lc.get("quantite_commandee", 0) or 0))
        total_dem = sum(dem.values())

        # servi
        serv: Dict[Any, float] = {}
        for m in mvts:
            if str(m.get("typemouvement","")).lower() != "sortie": continue
            dm = _parse_date(m.get("datemouvement"))
            if not _between(dm, win_start, d): continue
            pid = m.get("produit_id") or stock_prod_map.get(m.get("stock_id"))
            if pid is None: continue
            serv[pid] = serv.get(pid, 0.0) + max(0.0, float(m.get("quantite", 0) or 0))

        non_serv = sum(max(0.0, qd - min(qd, serv.get(pid, 0.0))) for pid, qd in dem.items())
        taux = round(100.0 * non_serv / total_dem, 2) if total_dem > 0 else 0.0
        out.append({"day": d.strftime("%Y-%m-%d"), "Rupture %": taux})
    return out

# ---------- 7) remaining shelf life avg (series) ----------
def rpc_remaining_shelf_life_series(tables, start_date: str | None = None, end_date: str | None = None, entrepot_id: Any | None = None):
    start, end = _resolve_window(start_date, end_date, default_days=6)
    days = _days_range(start, end)

    # snapshot (pas d'historique par jour sur peremption)
    ref = date.today()
    tot_q = tot_j = 0.0
    for r in (tables.get("peremption", []) or []):
        exp = _parse_date(r.get("dateexpiration"))
        if not exp: continue
        q = max(0.0, float(r.get("quantite", 0) or 0))
        j = max(0, (exp - ref).days)
        tot_q += q; tot_j += q * j
    avg = round(tot_j / tot_q, 1) if tot_q > 0 else 0.0

    return [{"day": d.strftime("%Y-%m-%d"), "Avg days": avg} for d in days]

# ---------- 8) contraction de stock (series) ----------
def rpc_shrinkage_by_day(tables, start_date: str | None = None, end_date: str | None = None, entrepot_id: Any | None = None):
    start, end = _resolve_window(start_date, end_date, default_days=6)
    days = _days_range(start, end)

    mvts_all = tables.get("mouvement_stock", []) or []
    stock_rows = tables.get("stock", []) or []
    mvts = _mvts_by_entrepot(mvts_all, stock_rows, entrepot_id)

    agg = { d.strftime("%Y-%m-%d"): 0.0 for d in days }
    for m in mvts:
        dm = _parse_date(m.get("datemouvement"))
        if not _between(dm, start, end): continue
        t = str(m.get("typemouvement","")).strip().lower()
        q = float(m.get("quantite", 0) or 0)
        if t.startswith("ajustement"):
            val = max(0.0, -q)              # ajustement négatif
        elif t in ("perte","shrink","shrinkage"):
            val = abs(q)
        else:
            val = 0.0
        agg[dm.strftime("%Y-%m-%d")] = agg.get(dm.strftime("%Y-%m-%d"), 0.0) + val

    return [{"day": day, "Quantité": round(agg[day], 2)} for day in sorted(agg)]



# -------- helpers communs (alignés avec tes autres modules) --------
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
    if start > end:
        start, end = end, start
    return start, end

def _days_range(start: date, end: date) -> List[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]

def _g(row: Dict[str, Any], *names: str, default=None):
    for n in names:
        if n in row and row[n] is not None:
            return row[n]
    return default

# Tables attendues:
# - commandefournisseur
# - reception_fournisseur
# - ligne_cmd_fournisseur
# - retour_fournisseur

# -------- 1) On-Time rate (livré à l'heure %) --------
def rpc_sup_on_time_rate_series(tables, start_date: str | None = None, end_date: str | None = None):
    start, end = _resolve_window(start_date, end_date, 6)
    days = _days_range(start, end)

    recs = tables.get("reception_fournisseur", []) or []
    cmds_by_id = {c.get("id"): c for c in (tables.get("commandefournisseur", []) or [])}

    out = []
    for d in days:
        win_start = d - timedelta(days=29)
        total = on_time = 0
        for r in recs:
            cmd = cmds_by_id.get(_g(r, "commande_id", "commandefournisseur_id", "id_commandefournisseur"))
            planned = _parse_date(_g(cmd or {}, "date_prevue_livraison", "date_prevue"))
            actual  = _parse_date(_g(r, "date_reception", "date_reelle_livraison")) or _parse_date(_g(cmd or {}, "date_reelle_livraison"))
            if not (planned and actual): 
                continue
            if not _between(actual, win_start, d):
                continue
            total += 1
            if actual <= planned:
                on_time += 1
        val = round(100.0 * on_time / total, 2) if total > 0 else 0.0
        out.append({"day": d.strftime("%Y-%m-%d"), "On-Time %": val})
    return out

# -------- 2) Conformité réceptions (%) --------
def rpc_sup_quality_conform_rate_series(tables, start_date: str | None = None, end_date: str | None = None):
    start, end = _resolve_window(start_date, end_date, 6)
    days = _days_range(start, end)

    recs = tables.get("reception_fournisseur", []) or []

    out = []
    for d in days:
        win_start = d - timedelta(days=29)
        total = ok = 0
        for r in recs:
            dr = _parse_date(_g(r, "date_reception", "date_reelle_livraison"))
            if not _between(dr, win_start, d): 
                continue
            statut = str(_g(r, "statut_conformite", "statutconformite", "")).lower()
            if not statut:
                continue
            total += 1
            if statut in ("conforme","ok","valide"):
                ok += 1
        val = round(100.0 * ok / total, 2) if total > 0 else 0.0
        out.append({"day": d.strftime("%Y-%m-%d"), "Conformité %": val})
    return out

# -------- 3) Non-conformité (quantités) / total reçues (%) --------
def rpc_sup_quality_nonconform_rate_series(tables, start_date: str | None = None, end_date: str | None = None):
    start, end = _resolve_window(start_date, end_date, 6)
    days = _days_range(start, end)

    lignes = tables.get("ligne_cmd_fournisseur", []) or []
    cmds_by_id = {c.get("id"): c for c in (tables.get("commandefournisseur", []) or [])}

    out = []
    for d in days:
        win_start = d - timedelta(days=29)
        q_total = q_nc = 0.0
        for l in lignes:
            cmd = cmds_by_id.get(_g(l, "commande_id", "commandefournisseur_id", "id_commandefournisseur"))
            dref = _parse_date(_g(cmd or {}, "date_reelle_livraison")) or _parse_date(_g(l, "date_reception"))
            if not _between(dref, win_start, d):
                continue
            qrec = float(_g(l, "quantite_recue", "quantiterecue", default=0) or 0)
            statut = str(_g(l, "statut_conformite", "statutconformite", "")).lower()
            q_total += max(0.0, qrec)
            if statut in ("nonconforme","non_conforme","rejet","defaut"):
                q_nc += max(0.0, qrec)
        val = round(100.0 * q_nc / q_total, 2) if q_total > 0 else 0.0
        out.append({"day": d.strftime("%Y-%m-%d"), "Non-conformité %": val})
    return out

# -------- 4) Taux de retours (%) --------
def rpc_sup_return_rate_series(tables, start_date: str | None = None, end_date: str | None = None):
    start, end = _resolve_window(start_date, end_date, 6)
    days = _days_range(start, end)

    retours = tables.get("retour_fournisseur", []) or []
    recs    = tables.get("reception_fournisseur", []) or []

    out = []
    for d in days:
        win_start = d - timedelta(days=29)
        nb_ret = sum(1 for r in retours if _between(_parse_date(_g(r, "date_retour","dateretour")), win_start, d))
        nb_rec = sum(1 for r in recs    if _between(_parse_date(_g(r, "date_reception","date_reelle_livraison")), win_start, d))
        val = round(100.0 * nb_ret / nb_rec, 2) if nb_rec > 0 else 0.0
        out.append({"day": d.strftime("%Y-%m-%d"), "Retours %": val})
    return out

# -------- 5) Lead time moyen (jours) --------
def rpc_sup_avg_lead_time_days_series(tables, start_date: str | None = None, end_date: str | None = None):
    start, end = _resolve_window(start_date, end_date, 6)
    days = _days_range(start, end)

    cmds = tables.get("commandefournisseur", []) or []
    recs = tables.get("reception_fournisseur", []) or []
    recs_by_cmd: Dict[Any, List[date]] = {}
    for r in recs:
        cid = _g(r, "commande_id", "commandefournisseur_id", "id_commandefournisseur")
        drec = _parse_date(_g(r, "date_reception", "date_reelle_livraison"))
        if cid is not None and drec:
            recs_by_cmd.setdefault(cid, []).append(drec)

    out = []
    for d in days:
        win_start = d - timedelta(days=29)
        delais: List[int] = []
        for c in cmds:
            dcmd = _parse_date(_g(c, "date_commande","datecommande"))
            drec = min(recs_by_cmd.get(c.get("id"), []), default=None) or _parse_date(_g(c, "date_reelle_livraison"))
            if not (dcmd and drec):
                continue
            if not _between(drec, win_start, d):
                continue
            delais.append((drec - dcmd).days)
        val = round(sum(delais)/len(delais), 2) if delais else 0.0
        out.append({"day": d.strftime("%Y-%m-%d"), "Lead time (j)": val})
    return out

# -------- 6) Coût transport / commande (ratio) --------
def rpc_sup_transport_cost_ratio_series(tables, start_date: str | None = None, end_date: str | None = None):
    start, end = _resolve_window(start_date, end_date, 6)
    days = _days_range(start, end)

    cmds = tables.get("commandefournisseur", []) or []

    out = []
    for d in days:
        win_start = d - timedelta(days=29)
        cost = base = 0.0
        for c in cmds:
            dref = _parse_date(_g(c, "date_reelle_livraison")) or _parse_date(_g(c, "date_commande","datecommande"))
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
