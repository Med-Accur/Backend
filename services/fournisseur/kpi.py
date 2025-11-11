# services/fournisseur/kpi.py
from __future__ import annotations
from datetime import datetime, date, timedelta
from typing import Any, Optional, Dict, List

# ---------- utilitaires déjà présents (garde-les) ----------
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

def _days_range(start: date, end: date) -> List[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]

# ---- même window que les charts ----
def _latest_sup_date(tables: Dict[str, Any]) -> Optional[date]:
    pool: List[date] = []
    for c in (tables.get("commande_fournisseur", []) or []):
        for k in ("date_reelle_livraison","date_prevue_livraison","date_commande",
                  "datereellelivraison","dateprevuelivraison","datecommande"):
            pool.append(_parse_date(c.get(k)))
    for r in (tables.get("reception_fournisseur", []) or []):
        for k in ("date_reception","date_reelle_livraison","datereception","datevalidation"):
            pool.append(_parse_date(r.get(k)))
    for ret in (tables.get("retour_fournisseur", []) or []):
        for k in ("date_retour","dateretour"):
            pool.append(_parse_date(ret.get(k)))
    pool = [d for d in pool if d]
    return max(pool) if pool else None

def _resolve_window_sup(tables: Dict[str, Any],
                        start_date: Optional[str],
                        end_date: Optional[str],
                        default_days: int = 29) -> tuple[date, date]:
    last = _latest_sup_date(tables) or date.today()
    end = _parse_date(end_date) or last
    start = _parse_date(start_date) or (end - timedelta(days=default_days))
    if start > end:
        start, end = end, start
    return start, end

# -----------------------------------
#             KPI RÉELS
# -----------------------------------

def get_sup_on_time_rate(tables: Dict[str, Any],
                         start_date: str | None = None,
                         end_date: str | None = None):
    """
    % réceptions livrées à temps (<= date prévue) sur la même fenêtre que les charts.
    """
    start, end = _resolve_window_sup(tables, start_date, end_date)

    recs = tables.get("reception_fournisseur", []) or []
    cmds_by_id = {c.get("id"): c for c in (tables.get("commande_fournisseur", []) or [])}

    def _planned_date(rec, cmd):
        # d'abord sur la réception, puis sur la commande (comme les séries)
        for k in ("date_prevue_livraison", "date_prevue", "date_prevue_reception"):
            d = _parse_date((rec or {}).get(k))
            if d: return d
        for k in ("date_prevue_livraison", "date_prevue"):
            d = _parse_date((cmd or {}).get(k))
            if d: return d
        return None

    def _actual_date(rec, cmd):
        for k in ("date_reception", "date_reelle_livraison"):
            d = _parse_date((rec or {}).get(k))
            if d: return d
        for k in ("date_reelle_livraison",):
            d = _parse_date((cmd or {}).get(k))
            if d: return d
        return None

    total = on_time = 0
    for r in recs:
        cmd = cmds_by_id.get(_g(r, "commande_id","commande_fournisseur_id","commandefournisseur_id","id_commandefournisseur"))
        planned = _planned_date(r, cmd)
        actual  = _actual_date(r, cmd)
        if not (planned and actual):
            continue
        if not _between(actual, start, end):
            continue
        total += 1
        if actual <= planned:
            on_time += 1

    return round(100.0 * on_time / total, 2) if total > 0 else 0.0


def get_sup_quality_conform_rate(tables: Dict[str, Any],
                                 start_date: str | None = None,
                                 end_date: str | None = None):
    """% réceptions conformes sur la fenêtre (aligné avec les charts)."""
    start, end = _resolve_window_sup(tables, start_date, end_date)
    recs = tables.get("reception_fournisseur", []) or []

    total = ok = 0
    for r in recs:
        d = _parse_date(_g(r, "date_reception","date_reelle_livraison","datereception","datevalidation"))
        if not _between(d, start, end):
            continue
        statut = str(_g(r, "statut_conformite","statutconformite","")).lower()
        if not statut:
            continue
        total += 1
        if statut in ("conforme","ok","valide"):
            ok += 1

    return round(100.0 * ok / total, 2) if total > 0 else 0.0


def get_sup_quality_nonconform_rate(tables: Dict[str, Any],
                                    start_date: str | None = None,
                                    end_date: str | None = None):
    """(Qté non conforme / Qté totale reçue) * 100 (mêmes clés/logic que chart)."""
    start, end = _resolve_window_sup(tables, start_date, end_date)
    lignes = tables.get("ligne_cmd_fournisseur", []) or []
    cmds_by_id = {c.get("id"): c for c in (tables.get("commande_fournisseur", []) or [])}

    q_total = q_nc = 0.0
    for l in lignes:
        cmd = cmds_by_id.get(_g(l, "commande_id","commande_fournisseur_id","commandefournisseur_id","id_commandefournisseur"))
        d = (
            _parse_date(_g(cmd or {}, "date_reelle_livraison","datereellelivraison"))
            or _parse_date(_g(l, "date_reception","datereception"))
        )
        if not _between(d, start, end):
            continue

        qrec = float(_g(l, "quantite_recue","quantiterecue", default=0) or 0)
        statut = str(_g(l, "statut_conformite","statutconformite","")).lower()

        q_total += max(0.0, qrec)
        if statut in ("nonconforme","non_conforme","rejet","defaut"):
            q_nc += max(0.0, qrec)

    return round(100.0 * q_nc / q_total, 2) if q_total > 0 else 0.0


def get_sup_return_rate(tables: Dict[str, Any],
                        start_date: str | None = None,
                        end_date: str | None = None):
    """nb retours / nb réceptions (fenêtre identique aux charts)."""
    start, end = _resolve_window_sup(tables, start_date, end_date)
    retours = tables.get("retour_fournisseur", []) or []
    recs    = tables.get("reception_fournisseur", []) or []

    nb_retours = sum(1 for r in retours if _between(_parse_date(_g(r, "date_retour","dateretour")), start, end))
    nb_recs    = sum(1 for r in recs    if _between(_parse_date(_g(r, "date_reception","date_reelle_livraison","datereception","datevalidation")), start, end))

    return round(100.0 * nb_retours / nb_recs, 2) if nb_recs > 0 else 0.0


def get_sup_avg_lead_time_days(tables: Dict[str, Any],
                               start_date: str | None = None,
                               end_date: str | None = None):
    """Lead time moyen (jours) = min(date_reception) - date_commande, par cmd dans la fenêtre (comme les charts)."""
    start, end = _resolve_window_sup(tables, start_date, end_date)

    cmds = tables.get("commande_fournisseur", []) or []
    recs = tables.get("reception_fournisseur", []) or []

    recs_by_cmd: Dict[Any, List[date]] = {}
    for r in recs:
        cid = _g(r, "commande_id","commande_fournisseur_id","commandefournisseur_id","id_commandefournisseur")
        drec = _parse_date(_g(r, "date_reception","date_reelle_livraison","datereception","datevalidation"))
        if cid is not None and drec:
            recs_by_cmd.setdefault(cid, []).append(drec)

    delais: List[int] = []
    for c in cmds:
        dcmd = _parse_date(_g(c, "date_commande","datecommande"))
        drec = min(recs_by_cmd.get(c.get("id"), []), default=None) or _parse_date(_g(c, "date_reelle_livraison","datereellelivraison"))
        if not (dcmd and drec):
            continue
        if not _between(drec, start, end):
            continue
        delais.append((drec - dcmd).days)

    return round(sum(delais) / len(delais), 2) if delais else 0.0


def get_sup_transport_cost_ratio(tables: Dict[str, Any],
                                 start_date: str | None = None,
                                 end_date: str | None = None):
    """Ratio coût transport / montant commande (même fenêtre que les charts)."""
    start, end = _resolve_window_sup(tables, start_date, end_date)
    cmds = tables.get("commande_fournisseur", []) or []

    cost = base = 0.0
    for c in cmds:
        dref = _parse_date(_g(c, "date_reelle_livraison","datereellelivraison")) or _parse_date(_g(c, "date_commande","datecommande"))
        if not _between(dref, start, end):
            continue
        cost += float(_g(c, "cout_transport","couttransport", default=0) or 0)
        base += float(_g(c, "montant_commande","montantcommande", default=0) or 0)

    return round(cost / base, 4) if base > 0 else 0.0
