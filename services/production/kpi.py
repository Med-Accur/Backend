from datetime import datetime

# ---------- Helpers robustes pour parser les dates ----------
def _parse_any_dt(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    s = str(value).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except Exception:
        pass
    fmts = [
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d",
    ]
    for f in fmts:
        try:
            return datetime.strptime(s, f)
        except Exception:
            continue
    return None

# ---------- Fonctions prod_* attendues par le front (un seul paramètre: tables) ----------

def prod_volume_ok(tables):
    sorties = tables.get("sortie_production", []) or []
    return sum(int(s.get("quantite_ok") or 0) for s in sorties)

def prod_volume_nok(tables):
    sorties = tables.get("sortie_production", []) or []
    return sum(int(s.get("quantite_nok") or 0) for s in sorties)

def prod_volume_total(tables):
    sorties = tables.get("sortie_production", []) or []
    return sum(int(s.get("quantite_ok") or 0) + int(s.get("quantite_nok") or 0) for s in sorties)

def prod_taux_qualite(tables):
    sorties = tables.get("sortie_production", []) or []
    ok = sum(int(s.get("quantite_ok") or 0) for s in sorties)
    nok = sum(int(s.get("quantite_nok") or 0) for s in sorties)
    total = ok + nok
    return round(100.0 * ok / total, 2) if total > 0 else None

def prod_taux_defauts(tables):
    sorties = tables.get("sortie_production", []) or []
    ok = sum(int(s.get("quantite_ok") or 0) for s in sorties)
    nok = sum(int(s.get("quantite_nok") or 0) for s in sorties)
    total = ok + nok
    return round(100.0 * nok / total, 2) if total > 0 else None

def prod_rendement_vs_cible(tables):
    ops = tables.get("ordre_production", []) or []
    sorties = tables.get("sortie_production", []) or []

    ok_par_op = {}
    for s in sorties:
        id_op = s.get("id_op")
        ok_par_op[id_op] = ok_par_op.get(id_op, 0) + int(s.get("quantite_ok") or 0)

    sum_ok = 0
    sum_cible = 0
    for op in ops:
        sum_ok += int(ok_par_op.get(op.get("id_op"), 0))
        sum_cible += int(op.get("quantite_cible") or 0)

    return round(100.0 * sum_ok / sum_cible, 2) if sum_cible > 0 else None

def prod_lead_time_of(tables):
    """
    Lead time moyen (heures) = date_fin_reelle - date_lancement_reelle
    Fallback: min(debut_reel)/max(fin_reel) des phases si déjà null.
    """
    ops = tables.get("ordre_production", []) or []
    phases = tables.get("phase_production", []) or []

    # min/max par OP via phases
    minmax = {}
    for p in phases:
        op_id = p.get("id_op")
        if not op_id:
            continue
        d = _parse_any_dt(p.get("debut_reel"))
        f = _parse_any_dt(p.get("fin_reel"))
        slot = minmax.setdefault(op_id, {"min": None, "max": None})
        if d and (slot["min"] is None or d < slot["min"]):
            slot["min"] = d
        if f and (slot["max"] is None or f > slot["max"]):
            slot["max"] = f

    hours = []
    for op in ops:
        start_dt = _parse_any_dt(op.get("date_lancement_reelle")) or (minmax.get(op.get("id_op"), {}).get("min"))
        end_dt   = _parse_any_dt(op.get("date_fin_reelle"))       or (minmax.get(op.get("id_op"), {}).get("max"))
        if start_dt and end_dt:
            h = (end_dt - start_dt).total_seconds() / 3600.0
            if h >= 0:
                hours.append(h)

    return round(sum(hours) / len(hours), 2) if hours else None

def prod_wip_op_en_cours(tables):
    ops = tables.get("ordre_production", []) or []
    return sum(1 for op in ops if op.get("etat") in ("en_attente", "en_cours"))
