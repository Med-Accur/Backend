# services/home/tables.py
from __future__ import annotations
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional
from collections import defaultdict

# -------- utils communes --------
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

def _g(row: Dict[str, Any], *names: str, default=None):
    for n in names:
        if n in row and row[n] is not None:
            return row[n]
    return default

# ============================================================
# 1) Commandes clients bloquées
#    Colonnes attendues par TABLE_TABLEAUX:
#    cmd_id, client, statut, date_commande, date_prevue_livraison,
#    date_reelle_livraison, retard_jours
# ============================================================
def table_cmd_clients_bloquees(tables, start_date: str|None=None, end_date: str|None=None):
    commandes = tables.get("commandeclient", []) or []
    contacts  = {c.get("id"): c for c in (tables.get("contact", []) or [])}

    out = []
    for c in commandes:
        cmd_id  = c.get("id")
        client  = (contacts.get(c.get("contact_id")) or {}).get("nom") or "Inconnu"
        statut  = c.get("statut")
        dcmd    = _parse_date(_g(c, "date_commande"))
        dprev   = _parse_date(_g(c, "date_prevue_livraison"))
        dreal   = _parse_date(_g(c, "date_reelle_livraison"))

        retard_j = 0
        if dprev and dreal and dreal > dprev:
            retard_j = (dreal - dprev).days

        # logique "bloquées" : statut non livré OU retard > 0
        bloque = (statut in ("en_attente", "en_cours", "expediee")) or (retard_j > 0)
        if not bloque:
            continue

        out.append({
            "cmd_id": cmd_id,
            "client": client,
            "statut": statut,
            "date_commande": dcmd.isoformat() if dcmd else None,
            "date_prevue_livraison": dprev.isoformat() if dprev else None,
            "date_reelle_livraison": dreal.isoformat() if dreal else None,
            "retard_jours": retard_j,
        })
    # tri par retard décroissant (le front a un default_sort, mais autant pretri)
    out.sort(key=lambda r: r.get("retard_jours", 0), reverse=True)
    return out

# ============================================================
# 2) Stock à risque de rupture
#    Colonnes: produit, reference, entrepot, stock_disponible, conso_30j,
#              days_on_hand, risque
# ============================================================
from datetime import date, timedelta
from collections import defaultdict

def table_stock_risque_rupture(
    tables, 
    start_date: str | None = None, 
    end_date: str | None = None
):
    """
    Retourne une table 'Stock à risque de rupture'.

    Colonnes: 
      - produit
      - reference
      - entrepot
      - stock_disponible
      - conso_30j
      - days_on_hand
      - risque  ('élevé' si DOH <= 3, 'moyen' si <= 7, sinon 'faible')
    Fenêtre de conso:
      - si start_date & end_date fournis -> [start_date, end_date]
      - sinon -> [today-29, today]
    """
    # --------- Fenêtre temporelle ---------
    today = date.today()
    if start_date and end_date:
        win_start = _parse_date(start_date) or (today - timedelta(days=29))
        win_end   = _parse_date(end_date)   or today
        if win_start > win_end:
            # on swap si inversé
            win_start, win_end = win_end, win_start
    else:
        win_start = today - timedelta(days=29)
        win_end   = today

    # --------- Référentiels ---------
    produits   = {p.get("id"): p for p in (tables.get("produit",   []) or [])}
    entrepots  = {e.get("id"): e for e in (tables.get("entrepot",  []) or [])}
    stock_rows =         tables.get("stock",               []) or []
    mvts       =         tables.get("mouvement_stock",     []) or []

    # --------- Conso 30j par stock_id via mouvements "Sortie*" ---------
    conso_by_stock = defaultdict(float)
    for m in mvts:
        typ = str(_g(m, "typemouvement", "type_mouvement", default="")).lower()
        if not typ.startswith("sortie"):
            continue
        dm = _parse_date(_g(m, "datemouvement", "date_mouvement"))
        if _between(dm, win_start, win_end):
            sid = m.get("stock_id")
            q   = float(_g(m, "quantite", "qte", default=0) or 0)
            if sid is not None and q > 0:
                conso_by_stock[sid] += q

    # --------- Construction de la sortie ---------
    out = []
    for s in stock_rows:
        sid = s.get("id")
        pid = s.get("produit_id")
        eid = s.get("entrepot_id")

        prod = produits.get(pid) or {}
        ent  = entrepots.get(eid) or {}

        # Champs tolérants (camelCase / snake_case)
        q_dispo = float(_g(s, "quantitedisponible", "quantiteDisponible", "quantite_disponible", default=0) or 0)
        q_res   = float(_g(s, "quantitereserve",    "quantiteReserve",    "quantite_reservee",  default=0) or 0)
        dispo   = max(0.0, q_dispo - q_res)

        conso = float(conso_by_stock.get(sid, 0.0))

        # Days On Hand: jours couverts au rythme de conso observé (lissé sur 30 jours)
        if conso > 0:
            doh = round(dispo / (conso / 30.0), 2)
        else:
            # pas de conso observée dans la fenêtre → DOH indéfini (on met 0 pour le tri)
            doh = 0.0

        # Niveaux de risque (seuils simples)
        if doh <= 3:
            risque = "élevé"
        elif doh <= 7:
            risque = "moyen"
        else:
            risque = "faible"

        out.append({
            "produit": prod.get("nom") or f"Produit {pid}",
            "reference": prod.get("reference"),
            "entrepot": ent.get("nom") or (f"Entrepôt {eid}" if eid is not None else None),
            "stock_disponible": round(dispo, 2),
            "conso_30j": round(conso, 2),
            "days_on_hand": doh,
            "risque": risque,
        })

    # --------- Tri: criticité puis DOH croissant ---------
    ordre = {"élevé": 0, "moyen": 1, "faible": 2}
    out.sort(key=lambda r: (ordre.get(r["risque"], 3), r["days_on_hand"]))

    return out


# ============================================================
# 3) Fournisseurs en retard
#    Colonnes: cmdf_id, fournisseur, date_commande, date_prevue_livraison,
#              date_reelle_livraison, retard_jours, statut_conformite
# ============================================================
def table_fournisseurs_retard(tables, start_date: str|None=None, end_date: str|None=None):
    from datetime import date
    from collections import defaultdict

    sdt = _parse_date(start_date) if start_date else None
    edt = _parse_date(end_date) if end_date else None

    cmds = tables.get("commande_fournisseur", []) \
        or tables.get("commandefournisseur", []) \
        or []
    recs = tables.get("reception_fournisseur", []) \
        or tables.get("receptionfournisseur", []) \
        or []
    tiers_tab = tables.get("tiers", []) or tables.get("tiers_type", []) or []
    tiers = {t.get("id"): t for t in tiers_tab}

    # agrégations
    min_rec_by_cmd = defaultdict(lambda: None)
    conformites_by_cmd = defaultdict(list)
    prio = {"Non conforme": 0, "Partiellement conforme": 1, "Conforme": 2}

    # ✅ CETTE BOUCLE DOIT ÊTRE DANS LA FONCTION (indentée)
    for r in recs:
        cid = _g(r, "commande_id", "commandefournisseur_id", "id_commandefournisseur")
        dr  = _parse_date(_g(r, "date_reception", "datereception", "date_reelle_livraison"))
        sc  = _g(r, "statut_conformite", "statutconformite")
        if cid is None:
            continue
        cur = min_rec_by_cmd[cid]
        if dr is not None and (cur is None or dr < cur):
            min_rec_by_cmd[cid] = dr
        if sc:
            conformites_by_cmd[cid].append(str(sc).strip())

    def agg_conformite(vals):
        if not vals:
            return None
        return sorted(vals, key=lambda v: prio.get(v, 99))[0]

    out = []
    for c in cmds:
        cid   = c.get("id")
        dcmd  = _parse_date(_g(c, "date_commande", "datecommande"))
        dprev = _parse_date(_g(c, "date_prevue_livraison", "date_prevue", "dateprevulivraison"))
        dreal = min_rec_by_cmd.get(cid) or _parse_date(_g(c, "date_reelle_livraison"))

        # filtre sur la période demandée (sur date_commande)
        if sdt and (not dcmd or dcmd < sdt):
            continue
        if edt and (not dcmd or dcmd > edt):
            continue

        trow = tiers.get(_g(c, "tiers_id", "fournisseur_id")) or {}
        fournisseur_nom = _g(trow, "raison_social", "raisonSociale", "nom", "name", default="Fournisseur")

        retard = 0
        if dprev and dreal and dreal > dprev:
            retard = (dreal - dprev).days

        en_retard = (retard > 0) or (dprev and not dreal and date.today() > dprev)
        if not en_retard:
            continue

        out.append({
            "cmdf_id": cid,
            "fournisseur": fournisseur_nom,
            "date_commande": dcmd.isoformat() if dcmd else None,
            "date_prevue_livraison": dprev.isoformat() if dprev else None,
            "date_reelle_livraison": dreal.isoformat() if dreal else None,
            "retard_jours": retard,
            "statut_conformite": agg_conformite(conformites_by_cmd.get(cid, [])),
        })

    out.sort(key=lambda r: r.get("retard_jours", 0), reverse=True)
    return out

# ============================================================
# 4) Péremption à 30 jours
#    Colonnes: produit, entrepot, date_expiration, jours_restants, quantite
# ============================================================
def table_peremption_30j(tables, seuil_jours: int = 30):
    today = date.today()
    produits = {p.get("id"): p for p in (tables.get("produit", []) or [])}
    entrepots = {e.get("id"): e for e in (tables.get("entrepot", []) or [])}
    stock_by_id = {s.get("id"): s for s in (tables.get("stock", []) or [])}

    out = []
    for r in (tables.get("peremption", []) or []):
        exp = _parse_date(_g(r, "dateexpiration", "date_expiration"))
        if not exp: 
            continue
        j = (exp - today).days
        if j < 0: j = 0
        if j > seuil_jours:
            continue

        q = float(_g(r, "quantite","qte", default=0) or 0)
        sid = r.get("stock_id")
        s   = stock_by_id.get(sid) or {}
        pid = r.get("produit_id") or s.get("produit_id")
        eid = s.get("entrepot_id")

        out.append({
            "produit": (produits.get(pid) or {}).get("nom") or f"Produit {pid}",
            "entrepot": (entrepots.get(eid) or {}).get("nom") or (f"Entrepôt {eid}" if eid else None),
            "date_expiration": exp.isoformat(),
            "jours_restants": j,
            "quantite": round(q, 2),
        })

    out.sort(key=lambda r: (r["jours_restants"], -r["quantite"]))
    return out
