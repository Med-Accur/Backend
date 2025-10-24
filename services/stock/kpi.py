# services/stock/kpi.py
from __future__ import annotations
from datetime import datetime, date, timedelta
from typing import Any, Optional, Dict, List


# =========================
# Utils dates / fenêtres
# =========================
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

def _window_30d(tables: Dict[str, Any], start_date: Optional[str], end_date: Optional[str]) -> tuple[date, date]:
    """
    Si end_date n'est pas donnée, on la déduit du max des dates connues
    (livraisons réelles, mouvements, péremptions). Sinon aujourd'hui.
    start = end - 29 (fenêtre 30 jours).
    """
    end = _parse_date(end_date)
    if not end:
        candidates: List[date] = []

        # commandeclient.date_reelle_livraison
        for c in (tables.get("commandeclient", []) or []):
            d = _parse_date(c.get("date_reelle_livraison"))
            if d: candidates.append(d)

        # mouvement_stock.datemouvement / date_mouvement
        for m in (tables.get("mouvement_stock", []) or []):
            d = _parse_date(m.get("datemouvement") or m.get("date_mouvement"))
            if d: candidates.append(d)

        # peremption.dateexpiration
        for p in (tables.get("peremption", []) or []):
            d = _parse_date(p.get("dateexpiration") or p.get("date_expiration"))
            if d: candidates.append(d)

        end = max(candidates) if candidates else date.today()

    start = _parse_date(start_date) or (end - timedelta(days=29))
    if start > end:
        start, end = end, start
    return start, end


# =========================
# Aides métier stock/ventes
# =========================
def _valeur_stock_courante(tables: Dict[str, Any]) -> float:
    """
    Valorisation courante = Σ (stock.quantitedisponible * produit.prix)
    (instantané — pas de reconstitution par jour)
    """
    produits = {p.get("id"): p for p in (tables.get("produit", []) or [])}
    total = 0.0
    for s in (tables.get("stock", []) or []):
        pid = s.get("produit_id")
        prix = float((produits.get(pid) or {}).get("prix", 0) or 0)
        q = float(s.get("quantitedisponible", 0) or 0)
        total += max(0.0, q) * max(0.0, prix)
    return float(total)

def _stock_disponible_now(tables: Dict[str, Any]) -> float:
    """
    Disponibilité actuelle = Σ max(0, quantitedisponible - quantitereserve)
    """
    total = 0.0
    for s in (tables.get("stock", []) or []):
        qd = float(s.get("quantitedisponible", 0) or 0)
        qr = float(s.get("quantitereserve", 0) or 0)
        total += max(0.0, qd - qr)
    return float(total)

def _ca_30j(tables: Dict[str, Any], start: date, end: date) -> float:
    """
    CA sur 30j = Σ (lignecommande.quantite_commandee * prix_unitaire)
    pour les commandes livrées réellement dans [start..end]
    """
    cmds = {c.get("id"): c for c in (tables.get("commandeclient", []) or [])}
    total = 0.0
    for lc in (tables.get("lignecommande", []) or []):
        cc = cmds.get(lc.get("commande_id"))
        d = _parse_date(cc.get("date_reelle_livraison")) if cc else None
        if not _between(d, start, end):
            continue
        q = float(lc.get("quantite_commandee", lc.get("quantite", 0)) or 0)
        pu = float(lc.get("prix_unitaire", lc.get("prix", 0)) or 0)
        total += max(0.0, q * pu)
    return float(total)

def _conso_sorties_30j(tables: Dict[str, Any], start: date, end: date) -> float:
    """
    Consommation = Σ des 'sorties' sur [start..end]
    On détecte les sorties par typemouvement commençant par 'sortie'.
    """
    total = 0.0
    for m in (tables.get("mouvement_stock", []) or []):
        typ = str(m.get("typemouvement", "")).strip().lower()
        if not typ.startswith("sortie"):
            continue
        d = _parse_date(m.get("datemouvement") or m.get("date_mouvement"))
        if not _between(d, start, end):
            continue
        total += max(0.0, float(m.get("quantite", 0) or 0))
    return float(total)


# =========================
# KPI — Disponibilité
# =========================
def get_kpi_quantite_stock(tables: Dict[str, Any], start_date: str | None = None, end_date: str | None = None):
    """
    Somme des quantités disponibles (instantané).
    Les dates ne changent pas la valeur (snapshot).
    """
    return float(sum(max(0.0, float(s.get("quantitedisponible", 0) or 0)) for s in (tables.get("stock", []) or [])))

def get_kpi_quantite_reservee(tables: Dict[str, Any], start_date: str | None = None, end_date: str | None = None):
    """
    Somme des quantités réservées (instantané).
    """
    return float(sum(max(0.0, float(s.get("quantitereserve", 0) or 0)) for s in (tables.get("stock", []) or [])))

def get_kpi_stock_disponible(tables: Dict[str, Any], start_date: str | None = None, end_date: str | None = None):
    """
    Disponibilité actuelle = Σ max(0, quantitedisponible - quantitereserve)
    """
    return _stock_disponible_now(tables)

def get_kpi_days_on_hand(tables: Dict[str, Any], start_date: str | None = None, end_date: str | None = None):
    """
    DOH = stock_disponible_now / (conso_moyenne_journalière sur 30j)
    conso_moyenne = (Σ sorties sur [end-29 .. end]) / 30
    """
    _, end = _window_30d(tables, start_date, end_date)
    start = end - timedelta(days=29)

    conso = _conso_sorties_30j(tables, start, end)
    if conso <= 0:
        return None
    avg = conso / 30.0
    stock_now = _stock_disponible_now(tables)
    return round(stock_now / avg, 2) if avg > 0 else None


# =========================
# KPI — Valorisation / Ventes
# =========================
def get_kpi_taux_rotation(tables: Dict[str, Any], start_date: str | None = None, end_date: str | None = None):
    """
    Taux de rotation = (coût des ventes ≈ CA 30j) / stock valorisé (instantané)
    """
    start, end = _window_30d(tables, start_date, end_date)
    ca = _ca_30j(tables, start, end)
    stock_val = _valeur_stock_courante(tables)
    if stock_val <= 0:
        return None
    return round(ca / stock_val, 2)

def get_kpi_inventory_to_sales(tables: Dict[str, Any], start_date: str | None = None, end_date: str | None = None):
    """
    Inventory to Sales = valeur stock (instantané) / CA 30j
    """
    start, end = _window_30d(tables, start_date, end_date)
    ca = _ca_30j(tables, start, end)
    if ca <= 0:
        return None
    return round(_valeur_stock_courante(tables) / ca, 2)

def get_kpi_rentabilite_stock(
    tables: Dict[str, Any],
    start_date: str | None = None,
    end_date: str | None = None,
    cout_ratio: float = 0.7,
    couts_logistiques: float = 0.0,
):
    """
    Rentabilité du stock = (CA - coût des ventes - coûts logistiques) / stock valorisé
    où coût des ventes ≈ CA * cout_ratio
    """
    start, end = _window_30d(tables, start_date, end_date)
    ca = _ca_30j(tables, start, end)
    cdv = ca * max(0.0, min(1.0, float(cout_ratio)))
    marge = ca - cdv - max(0.0, float(couts_logistiques))
    stock_val = _valeur_stock_courante(tables)
    if stock_val <= 0:
        return None
    return round(marge / stock_val, 2)


# =========================
# KPI — Ruptures & Péremption
# =========================
def get_kpi_taux_rupture(tables: Dict[str, Any], start_date: str | None = None, end_date: str | None = None):
    """
    Rupture% = (Demande_non_servie / Demande_totale) * 100 sur 30j
    Demande = Σ quantités commandées (lignes livrées [30j])
    Servi   = Σ 'sorties' [30j] (par produit)
    """
    start, end = _window_30d(tables, start_date, end_date)

    # Demande par produit
    cmds = {c.get("id"): c for c in (tables.get("commandeclient", []) or [])}
    demande: Dict[Any, float] = {}
    for lc in (tables.get("lignecommande", []) or []):
        cc = cmds.get(lc.get("commande_id"))
        d = _parse_date(cc.get("date_reelle_livraison")) if cc else None
        if not _between(d, start, end):
            continue
        pid = lc.get("produit_id")
        q = float(lc.get("quantite_commandee", lc.get("quantite", 0)) or 0)
        demande[pid] = demande.get(pid, 0.0) + max(0.0, q)

    total_dem = sum(demande.values())
    if total_dem <= 0:
        return None

    # Servi via mouvements 'sortie'
    mvts = (tables.get("mouvement_stock", []) or [])
    has_ms_prod = any("produit_id" in m for m in mvts)
    stock_map = {s.get("id"): s.get("produit_id") for s in (tables.get("stock", []) or [])}

    servi: Dict[Any, float] = {}
    for m in mvts:
        typ = str(m.get("typemouvement", "")).strip().lower()
        if not typ.startswith("sortie"):
            continue
        d = _parse_date(m.get("datemouvement") or m.get("date_mouvement"))
        if not _between(d, start, end):
            continue
        pid = m.get("produit_id") if has_ms_prod else stock_map.get(m.get("stock_id"))
        if pid is None:
            continue
        q = float(m.get("quantite", 0) or 0)
        servi[pid] = servi.get(pid, 0.0) + max(0.0, q)

    non_serv = sum(max(0.0, qd - min(qd, servi.get(pid, 0.0))) for pid, qd in demande.items())
    return round(100.0 * non_serv / total_dem, 2)

def get_kpi_remaining_shelf_life_avg(tables: Dict[str, Any], start_date: str | None = None, end_date: str | None = None):
    """
    Moyenne pondérée (en jours) des durées restantes de péremption,
    calculée au 'ref' = end_date (ou max data / aujourd'hui).
    """
    _, ref = _window_30d(tables, start_date, end_date)
    tot_q = tot_j = 0.0
    for r in (tables.get("peremption", []) or []):
        exp = _parse_date(r.get("dateexpiration") or r.get("date_expiration"))
        if not exp:
            continue
        q = max(0.0, float(r.get("quantite", 0) or 0))
        j = max(0, (exp - ref).days)
        tot_q += q
        tot_j += q * j
    return round(tot_j / tot_q, 1) if tot_q > 0 else None

def get_kpi_produits_proches_peremption(tables: Dict[str, Any], start_date: str | None = None, end_date: str | None = None):
    """
    Liste des produits dont la péremption <= 30 jours (à ref = end_date).
    """
    _, ref = _window_30d(tables, start_date, end_date)
    seuil_jours = 30
    out: List[Dict[str, Any]] = []
    for r in (tables.get("peremption", []) or []):
        exp = _parse_date(r.get("dateexpiration") or r.get("date_expiration"))
        if not exp:
            continue
        j = (exp - ref).days
        if j <= seuil_jours:
            out.append({
                "produit_id": r.get("produit_id"),
                "jours_restants": max(0, j),
                "quantite": float(r.get("quantite", 0) or 0),
            })
    return out

def get_kpi_contraction_stock_qte(tables: Dict[str, Any], start_date: str | None = None, end_date: str | None = None):
    """
    Contraction (shrinkage) sur 30j = Σ pertes + ajustements négatifs
    """
    start, end = _window_30d(tables, start_date, end_date)
    total = 0.0
    for m in (tables.get("mouvement_stock", []) or []):
        d = _parse_date(m.get("datemouvement") or m.get("date_mouvement"))
        if not _between(d, start, end):
            continue
        t = str(m.get("typemouvement", "")).strip().lower()
        q = float(m.get("quantite", 0) or 0)
        if t in ("perte", "shrink", "shrinkage"):
            total += abs(q)
        elif t.startswith("ajustement") and q < 0:
            total += -q
    return float(round(total, 2))
