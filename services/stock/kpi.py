# services/stock/kpi_simple.py
from __future__ import annotations
from datetime import datetime, date, timedelta
from typing import Any, Optional, Dict, List

# ---------- utils ----------
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

def _last30_window(tables: Dict[str, Any]) -> tuple[date, date]:
    # fin = max(date_reelle_livraison) sinon aujourd’hui ; début = fin - 29
    ccs = tables.get("commandeclient", []) or []
    dates = [_parse_date(c.get("date_reelle_livraison")) for c in ccs]
    dates = [d for d in dates if d]
    end = max(dates) if dates else date.today()
    start = end - timedelta(days=29)
    return start, end

# ---------- KPI "disponibilité" ----------
def get_kpi_quantite_stock(tables):
    return float(sum(max(0, s.get("quantitedisponible", 0)) for s in (tables.get("stock", []) or [])))

def get_kpi_quantite_reservee(tables):
    return float(sum(max(0, s.get("quantitereserve", 0)) for s in (tables.get("stock", []) or [])))

def get_kpi_stock_disponible(tables):
    return float(sum(
        max(0, (s.get("quantitedisponible", 0) or 0) - (s.get("quantitereserve", 0) or 0))
        for s in (tables.get("stock", []) or [])
    ))

# Days On Hand = stock_actuel / conso_moy_jour (sur 30 jours glissants)
def get_kpi_days_on_hand(tables):
    start, end = _last30_window(tables)
    mvts = tables.get("mouvement_stock", []) or []
    conso = sum(
        max(0.0, float(m.get("quantite", 0) or 0))
        for m in mvts
        if str(m.get("typemouvement", "")).lower() == "sortie"
        and _between(_parse_date(m.get("datemouvement")), start, end)
    )
    if conso <= 0:
        return None
    avg = conso / 30.0
    stock_actuel = get_kpi_stock_disponible(tables)
    return round(stock_actuel / avg, 2) if avg > 0 else None

# ---------- valorisation/ventes ----------
def _valeur_stock_courante(tables) -> float:
    produits = {p.get("id"): p for p in (tables.get("produit", []) or [])}
    total = 0.0
    for s in (tables.get("stock", []) or []):
        prix = float((produits.get(s.get("produit_id")) or {}).get("prix", 0) or 0)
        total += max(0, s.get("quantitedisponible", 0)) * max(0.0, prix)
    return float(total)

# Taux de rotation = coût des ventes / stock moyen (≈ stock courant valorisé), fenêtre 30j
def get_kpi_taux_rotation(tables):
    start, end = _last30_window(tables)
    cmds = {c.get("id"): c for c in (tables.get("commandeclient", []) or [])}
    cout_ventes = 0.0
    for lc in (tables.get("lignecommande", []) or []):
        cc = cmds.get(lc.get("commande_id"))
        if cc and _between(_parse_date(cc.get("date_reelle_livraison")), start, end):
            q = float(lc.get("quantite_commandee", 0) or 0)
            pu = float(lc.get("prix_unitaire", 0) or 0)
            cout_ventes += max(0.0, q * pu)
    stock_moyen = _valeur_stock_courante(tables)
    if stock_moyen <= 0:
        return None
    return round(cout_ventes / stock_moyen, 2)

# Inventory to Sales = valeur stock / chiffre d’affaires (30j)
def get_kpi_inventory_to_sales(tables):
    start, end = _last30_window(tables)
    cmds = {c.get("id"): c for c in (tables.get("commandeclient", []) or [])}
    ventes = 0.0
    for lc in (tables.get("lignecommande", []) or []):
        cc = cmds.get(lc.get("commande_id"))
        if cc and _between(_parse_date(cc.get("date_reelle_livraison")), start, end):
            ventes += max(0.0, float(lc.get("quantite_commandee", 0) or 0) * float(lc.get("prix_unitaire", 0) or 0))
    if ventes <= 0:
        return None
    return round(_valeur_stock_courante(tables) / ventes, 2)

# Rentabilité du stock = (CA - coût des ventes - coûts logistiques) / stock moyen (30j)
def get_kpi_rentabilite_stock(tables):
    start, end = _last30_window(tables)
    cout_ratio = 0.7           # coût des ventes ≈ 70% du CA (par défaut)
    couts_logistiques = 0.0
    cmds = {c.get("id"): c for c in (tables.get("commandeclient", []) or [])}
    ca = 0.0
    for lc in (tables.get("lignecommande", []) or []):
        cc = cmds.get(lc.get("commande_id"))
        if cc and _between(_parse_date(cc.get("date_reelle_livraison")), start, end):
            ca += max(0.0, float(lc.get("quantite_commandee", 0) or 0) * float(lc.get("prix_unitaire", 0) or 0))
    cdv = ca * max(0.0, min(1.0, cout_ratio))
    marge = ca - cdv - max(0.0, couts_logistiques)
    stock_moy = _valeur_stock_courante(tables)
    if stock_moy <= 0:
        return None
    return round(marge / stock_moy, 2)

# ---------- ruptures & péremption ----------
def get_kpi_taux_rupture(tables):
    start, end = _last30_window(tables)
    cmds = {c.get("id"): c for c in (tables.get("commandeclient", []) or [])}
    # demande par produit (30j)
    dem: Dict[Any, float] = {}
    for lc in (tables.get("lignecommande", []) or []):
        cc = cmds.get(lc.get("commande_id"))
        d = _parse_date(cc.get("date_reelle_livraison")) if cc else None
        if not _between(d, start, end): continue
        pid = lc.get("produit_id")
        dem[pid] = dem.get(pid, 0.0) + max(0.0, float(lc.get("quantite_commandee", 0) or 0))
    total = sum(dem.values())
    if total <= 0:
        return None
    # servie via sorties (30j)
    mvts = tables.get("mouvement_stock", []) or []
    has_ms_prod = any("produit_id" in m for m in mvts)
    stock_map = {s.get("id"): s.get("produit_id") for s in (tables.get("stock", []) or [])}
    serv: Dict[Any, float] = {}
    for m in mvts:
        if str(m.get("typemouvement","")).lower() != "sortie": continue
        if not _between(_parse_date(m.get("datemouvement")), start, end): continue
        pid = m.get("produit_id") if has_ms_prod else stock_map.get(m.get("stock_id"))
        if pid is None: continue
        serv[pid] = serv.get(pid, 0.0) + max(0.0, float(m.get("quantite", 0) or 0))
    non_serv = sum(max(0.0, qd - min(qd, serv.get(pid, 0.0))) for pid, qd in dem.items())
    return round(100.0 * non_serv / total, 2)

def get_kpi_remaining_shelf_life_avg(tables):
    ref = date.today()
    tot_q = tot_j = 0.0
    for r in (tables.get("peremption", []) or []):
        exp = _parse_date(r.get("dateexpiration"))
        if not exp: continue
        q = max(0.0, float(r.get("quantite", 0) or 0))
        j = max(0, (exp - ref).days)
        tot_q += q; tot_j += q * j
    return round(tot_j / tot_q, 1) if tot_q > 0 else None

def get_kpi_produits_proches_peremption(tables):
    seuil_jours = 30
    ref = date.today()
    out: List[Dict[str, Any]] = []
    for r in (tables.get("peremption", []) or []):
        exp = _parse_date(r.get("dateexpiration"))
        if not exp: continue
        j = (exp - ref).days
        if j <= seuil_jours:
            out.append({
                "produit_id": r.get("produit_id"),
                "jours_restants": max(0, j),
                "quantite": float(r.get("quantite", 0) or 0),
            })
    return out

def get_kpi_contraction_stock_qte(tables):
    start, end = _last30_window(tables)
    total = 0.0
    for m in (tables.get("mouvement_stock", []) or []):
        if not _between(_parse_date(m.get("datemouvement")), start, end): continue
        t = str(m.get("typemouvement","")).strip().lower()
        q = float(m.get("quantite", 0) or 0)
        if t.startswith("ajustement"):       # ajustement négatif
            total += max(0.0, -q)
        elif t in ("perte","shrink","shrinkage"):
            total += abs(q)
    return float(round(total, 2))