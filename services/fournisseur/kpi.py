# services/fournisseur/kpi.py
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

def _g(row: Dict[str, Any], *names: str, default=None):
    for n in names:
        if n in row and row[n] is not None:
            return row[n]
    return default

def _window_30d(
    tables: Dict[str, Any],
    start_date: Optional[str],
    end_date: Optional[str],
) -> tuple[date, date]:
    """
    Si end_date/start_date ne sont pas fournis:
    - end = max(date_reception, date_reelle_livraison, date_commande) sinon aujourd’hui
    - start = end - 29
    """
    end = _parse_date(end_date)
    if not end:
        pool: List[date] = []
        for r in (tables.get("reception_fournisseur", []) or []):
            d = _parse_date(_g(r, "date_reception", "date_reelle_livraison"))
            if d: pool.append(d)
        for c in (tables.get("commande_fournisseur", []) or []):
            d = (
                _parse_date(_g(c, "date_reelle_livraison")) or
                _parse_date(_g(c, "date_prevue_livraison", "date_prevue")) or
                _parse_date(_g(c, "date_commande", "datecommande"))
            )
            if d: pool.append(d)
        end = max(pool) if pool else date.today()

    start = _parse_date(start_date) or (end - timedelta(days=29))
    if start > end:
        start, end = end, start
    return start, end


# =========================
# KPIs — Fournisseurs
# =========================
def get_sup_on_time_rate(
    tables: Dict[str, Any],
    start_date: str | None = None,
    end_date: str | None = None,
):
    """% réceptions livrées à temps (<= date prévue) sur la fenêtre 30j."""
    start, end = _window_30d(tables, start_date, end_date)

    recs = tables.get("reception_fournisseur", []) or []
    cmds_by_id = {c.get("id"): c for c in (tables.get("commande_fournisseur", []) or [])}

    total = on_time = 0
    for r in recs:
        cmd = cmds_by_id.get(_g(r, "commande_id", "commande_fournisseur_id", "commandefournisseur_id", "id_commandefournisseur"))
        planned = _parse_date(_g(cmd or {}, "date_prevue_livraison", "date_prevue"))
        actual  = _parse_date(_g(r, "date_reception", "date_reelle_livraison")) or _parse_date(_g(cmd or {}, "date_reelle_livraison"))
        if not (planned and actual):
            continue
        if not _between(actual, start, end):
            continue
        total += 1
        if actual <= planned:
            on_time += 1

    return round(100.0 * on_time / total, 2) if total > 0 else None


def get_sup_quality_conform_rate(
    tables: Dict[str, Any],
    start_date: str | None = None,
    end_date: str | None = None,
):
    """% réceptions conformes sur la fenêtre."""
    start, end = _window_30d(tables, start_date, end_date)

    recs = tables.get("reception_fournisseur", []) or []
    total = ok = 0
    for r in recs:
        d = _parse_date(_g(r, "date_reception", "date_reelle_livraison"))
        if not _between(d, start, end):
            continue
        statut = str(_g(r, "statut_conformite", "statutconformite", "")).lower()
        if not statut:
            continue
        total += 1
        if statut in ("conforme", "ok", "valide"):
            ok += 1

    return round(100.0 * ok / total, 2) if total > 0 else None


def get_sup_quality_nonconform_rate(
    tables: Dict[str, Any],
    start_date: str | None = None,
    end_date: str | None = None,
):
    """(Quantité non conforme / Quantité totale reçue) * 100 sur la fenêtre."""
    start, end = _window_30d(tables, start_date, end_date)

    lignes = tables.get("ligne_cmd_fournisseur", []) or []
    cmds_by_id = {c.get("id"): c for c in (tables.get("commande_fournisseur", []) or [])}

    q_total = q_nc = 0.0
    for l in lignes:
        cmd = cmds_by_id.get(_g(l, "commande_id", "commande_fournisseur_id", "commandefournisseur_id", "id_commandefournisseur"))
        d = _parse_date(_g(cmd or {}, "date_reelle_livraison")) or _parse_date(_g(l, "date_reception"))
        if not _between(d, start, end):
            continue

        qrec = float(_g(l, "quantite_recue", "quantiterecue", default=0) or 0)
        statut = str(_g(l, "statut_conformite", "statutconformite", "")).lower()

        q_total += max(0.0, qrec)
        if statut in ("nonconforme", "non_conforme", "rejet", "defaut"):
            q_nc += max(0.0, qrec)

    return round(100.0 * q_nc / q_total, 2) if q_total > 0 else None


def get_sup_return_rate(
    tables: Dict[str, Any],
    start_date: str | None = None,
    end_date: str | None = None,
):
    """Taux de retours = nb retours / nb réceptions sur la fenêtre."""
    start, end = _window_30d(tables, start_date, end_date)

    retours = tables.get("retour_fournisseur", []) or []
    recs    = tables.get("reception_fournisseur", []) or []

    nb_retours = sum(1 for r in retours if _between(_parse_date(_g(r, "date_retour", "dateretour")), start, end))
    nb_recs    = sum(1 for r in recs    if _between(_parse_date(_g(r, "date_reception", "date_reelle_livraison")), start, end))

    return round(100.0 * nb_retours / nb_recs, 2) if nb_recs > 0 else None


def get_sup_avg_lead_time_days(
    tables: Dict[str, Any],
    start_date: str | None = None,
    end_date: str | None = None,
):
    """Lead time moyen (en jours) = min(date_reception) - date_commande, par commande dans la fenêtre."""
    start, end = _window_30d(tables, start_date, end_date)

    cmds = tables.get("commande_fournisseur", []) or []
    recs = tables.get("reception_fournisseur", []) or []

    # réceptions par commande
    recs_by_cmd: Dict[Any, List[date]] = {}
    for r in recs:
        cid = _g(r, "commande_id", "commande_fournisseur_id", "commandefournisseur_id", "id_commandefournisseur")
        drec = _parse_date(_g(r, "date_reception", "date_reelle_livraison"))
        if cid is not None and drec:
            recs_by_cmd.setdefault(cid, []).append(drec)

    delais: List[int] = []
    for c in cmds:
        dcmd = _parse_date(_g(c, "date_commande", "datecommande"))
        drec = min(recs_by_cmd.get(c.get("id"), []), default=None) or _parse_date(_g(c, "date_reelle_livraison"))
        if not (dcmd and drec):
            continue
        if not _between(drec, start, end):
            continue
        delais.append((drec - dcmd).days)

    return round(sum(delais) / len(delais), 2) if delais else None


def get_sup_transport_cost_ratio(
    tables: Dict[str, Any],
    start_date: str | None = None,
    end_date: str | None = None,
):
    """Ratio coût transport / montant commande, agrégé sur la fenêtre."""
    start, end = _window_30d(tables, start_date, end_date)

    cmds = tables.get("commande_fournisseur", []) or []
    cost = base = 0.0
    for c in cmds:
        dref = (
            _parse_date(_g(c, "date_reelle_livraison")) or
            _parse_date(_g(c, "date_commande", "datecommande"))
        )
        if not _between(dref, start, end):
            continue
        cost += float(_g(c, "cout_transport", "couttransport", default=0) or 0)
        base += float(_g(c, "montant_commande", "montantcommande", default=0) or 0)

    return round(cost / base, 4) if base > 0 else None
