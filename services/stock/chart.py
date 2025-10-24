from __future__ import annotations
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional
from collections import defaultdict


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


