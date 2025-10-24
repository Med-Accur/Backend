from __future__ import annotations
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional


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
