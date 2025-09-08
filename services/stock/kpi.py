# services/stock/kpi.py
from __future__ import annotations
from datetime import datetime, date, timedelta
from typing import Dict, Any, Optional, List

# --------- utils ----------
def _parse_date(v: Any) -> Optional[date]:
    if v is None or v == "": return None
    if isinstance(v, date):  return v
    s = str(v)
    for fmt in ("%Y-%m-%d","%Y/%m/%d","%d/%m/%Y","%Y-%m-%d %H:%M:%S","%Y-%m-%dT%H:%M:%S"):
        try: return datetime.strptime(s[:len(fmt)], fmt).date()
        except: pass
    try: return datetime.fromisoformat(s).date()
    except: return None

def _between(d: Optional[date], a: Optional[date], b: Optional[date]) -> bool:
    if not d: return False
    if a and d < a: return False
    if b and d > b: return False
    return True

# --------- KPI dispo ----------

def get_kpi_quantite_stock(tables: Dict[str, Any]) -> float:
    return float(sum(max(0, s.get("quantitedisponible", 0)) for s in tables.get("stock", []) or []))

def get_kpi_quantite_reservee(tables: Dict[str, Any]) -> float:
    return float(sum(max(0, s.get("quantitereserve", 0)) for s in tables.get("stock", []) or []))

def get_kpi_stock_disponible(tables: Dict[str, Any]) -> float:
    return float(sum(max(0, (s.get("quantitedisponible", 0) or 0) - (s.get("quantitereserve", 0) or 0)) for s in tables.get("stock", []) or []))

def get_kpi_days_on_hand(tables: Dict[str, Any], period_days: int = 30, ref_date: Optional[str] = None) -> Optional[float]:
    end = _parse_date(ref_date) or datetime.utcnow().date()
    start = end - timedelta(days=max(period_days, 1)-1)
    sorties = [
        max(0, m.get("quantite", 0))
        for m in (tables.get("mouvement_stock", []) or [])
        if str(m.get("typemouvement","")).lower() == "sortie" and _between(_parse_date(m.get("datemouvement")), start, end)
    ]
    conso = sum(sorties)
    if conso <= 0: return None
    avg = conso / max(period_days, 1)
    stock_actuel = get_kpi_stock_disponible(tables)
    return round(stock_actuel / avg, 2) if avg > 0 else None

def _valeur_stock_courante(tables: Dict[str, Any]) -> float:
    produits = {p.get("id"): p for p in (tables.get("produit", []) or [])}
    total = 0.0
    for s in (tables.get("stock", []) or []):
        prix = float((produits.get(s.get("produit_id")) or {}).get("prix", 0) or 0)
        total += max(0, s.get("quantitedisponible", 0)) * max(0.0, prix)
    return float(total)

def get_kpi_taux_rotation(tables: Dict[str, Any], start_date: str, end_date: str) -> Optional[float]:
    a, b = _parse_date(start_date), _parse_date(end_date)
    if not (a and b and a <= b): return None
    cmds = {c.get("id"): c for c in (tables.get("commandeclient", []) or [])}
    cout_ventes = 0.0
    for lc in (tables.get("lignecommande", []) or []):
        cc = cmds.get(lc.get("commande_id"))
        if cc and _between(_parse_date(cc.get("date_reelle_livraison")), a, b):
            q = float(lc.get("quantite_commandee", 0) or 0)
            pu = float(lc.get("prix_unitaire", 0) or 0)
            cout_ventes += max(0.0, q * pu)
    stock_moyen = _valeur_stock_courante(tables)
    if stock_moyen <= 0: return None
    return round(cout_ventes / stock_moyen, 2)

def get_kpi_inventory_to_sales(tables: Dict[str, Any], start_date: str, end_date: str) -> Optional[float]:
    a, b = _parse_date(start_date), _parse_date(end_date)
    if not (a and b and a <= b): return None
    cmds = {c.get("id"): c for c in (tables.get("commandeclient", []) or [])}
    ventes = 0.0
    for lc in (tables.get("lignecommande", []) or []):
        cc = cmds.get(lc.get("commande_id"))
        if cc and _between(_parse_date(cc.get("date_reelle_livraison")), a, b):
            ventes += max(0.0, float(lc.get("quantite_commandee", 0) or 0) * float(lc.get("prix_unitaire", 0) or 0))
    if ventes <= 0: return None
    return round(_valeur_stock_courante(tables) / ventes, 2)

def get_kpi_rentabilite_stock(tables: Dict[str, Any], start_date: str, end_date: str, cout_ratio: float = 0.7, couts_logistiques: float = 0.0) -> Optional[float]:
    a, b = _parse_date(start_date), _parse_date(end_date)
    if not (a and b and a <= b): return None
    cmds = {c.get("id"): c for c in (tables.get("commandeclient", []) or [])}
    ca = 0.0
    for lc in (tables.get("lignecommande", []) or []):
        cc = cmds.get(lc.get("commande_id"))
        if cc and _between(_parse_date(cc.get("date_reelle_livraison")), a, b):
            ca += max(0.0, float(lc.get("quantite_commandee", 0) or 0) * float(lc.get("prix_unitaire", 0) or 0))
    cdv = ca * max(0.0, min(1.0, cout_ratio))
    marge = ca - cdv - max(0.0, couts_logistiques)
    stock_moy = _valeur_stock_courante(tables)
    if stock_moy <= 0: return None
    return round(marge / stock_moy, 2)

def get_kpi_taux_rupture(tables: Dict[str, Any], start_date: str | None = None, end_date: str | None = None) -> Optional[float]:
    a = _parse_date(start_date) if start_date else None
    b = _parse_date(end_date) if end_date else None
    cmds = {c.get("id"): c for c in (tables.get("commandeclient", []) or [])}
    # demandes par produit
    dem = {}
    for lc in (tables.get("lignecommande", []) or []):
        cc = cmds.get(lc.get("commande_id"))
        d = _parse_date(cc.get("date_reelle_livraison")) if cc else None
        if (a or b) and not _between(d, a, b): continue
        pid = lc.get("produit_id")
        dem[pid] = dem.get(pid, 0.0) + max(0.0, float(lc.get("quantite_commandee", 0) or 0))
    total = sum(dem.values())
    if total <= 0: return None
    # servies via sorties
    mvts = tables.get("mouvement_stock", []) or []
    has_ms_prod = any("produit_id" in m for m in mvts)
    stock_map = {s.get("id"): s.get("produit_id") for s in (tables.get("stock", []) or [])}
    serv = {}
    for m in mvts:
        if str(m.get("typemouvement","")).lower() != "sortie": continue
        d = _parse_date(m.get("datemouvement"))
        if (a or b) and not _between(d, a, b): continue
        pid = m.get("produit_id") if has_ms_prod else stock_map.get(m.get("stock_id"))
        if pid is None: continue
        serv[pid] = serv.get(pid, 0.0) + max(0.0, float(m.get("quantite", 0) or 0))
    non_serv = sum(max(0.0, qd - min(qd, serv.get(pid, 0.0))) for pid, qd in dem.items())
    return round(100.0 * non_serv / total, 2)

def get_kpi_remaining_shelf_life_avg(tables: Dict[str, Any], ref_date: str | None = None) -> Optional[float]:
    ref = _parse_date(ref_date) or datetime.utcnow().date()
    tot_q = tot_j = 0.0
    for r in (tables.get("peremption", []) or []):
        exp = _parse_date(r.get("dateexpiration"))
        if not exp: continue
        q = max(0.0, float(r.get("quantite", 0) or 0))
        j = max(0, (exp - ref).days)
        tot_q += q; tot_j += q * j
    return round(tot_j / tot_q, 1) if tot_q > 0 else None

def get_kpi_produits_proches_peremption(tables: Dict[str, Any], seuil_jours: int = 30, ref_date: str | None = None) -> List[Dict[str, Any]]:
    ref = _parse_date(ref_date) or datetime.utcnow().date()
    out = []
    for r in (tables.get("peremption", []) or []):
        exp = _parse_date(r.get("dateexpiration"))
        if not exp: continue
        j = (exp - ref).days
        if j <= seuil_jours:
            out.append({"produit_id": r.get("produit_id"), "jours_restants": max(0, j), "quantite": float(r.get("quantite", 0) or 0)})
    return out

def get_kpi_contraction_stock_qte(tables: Dict[str, Any], start_date: str, end_date: str) -> float:
    a, b = _parse_date(start_date), _parse_date(end_date)
    if not (a and b and a <= b): return 0.0
    total = 0.0
    for m in (tables.get("mouvement_stock", []) or []):
        if not _between(_parse_date(m.get("datemouvement")), a, b): continue
        t = str(m.get("typemouvement","")).strip().lower()
        q = float(m.get("quantite", 0) or 0)
        if t.startswith("ajustement"):
            total += max(0.0, -q)
        elif t in ("perte","shrink","shrinkage"):
            total += abs(q)
    return float(round(total, 2))

KPI_REGISTRY = {
    "get_kpi_quantite_stock": get_kpi_quantite_stock,
    "get_kpi_quantite_reservee": get_kpi_quantite_reservee,
    "get_kpi_stock_disponible": get_kpi_stock_disponible,
    "get_kpi_days_on_hand": get_kpi_days_on_hand,
    "get_kpi_taux_rotation": get_kpi_taux_rotation,
    "get_kpi_inventory_to_sales": get_kpi_inventory_to_sales,
    "get_kpi_rentabilite_stock": get_kpi_rentabilite_stock,
    "get_kpi_taux_rupture": get_kpi_taux_rupture,
    "get_kpi_remaining_shelf_life_avg": get_kpi_remaining_shelf_life_avg,
    "get_kpi_produits_proches_peremption": get_kpi_produits_proches_peremption,
    "get_kpi_contraction_stock_qte": get_kpi_contraction_stock_qte,
}
