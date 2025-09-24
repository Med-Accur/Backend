from datetime import datetime

def get_kpi_volume_production(tables):
    ofs = tables.get("ordre_fabrication", [])
    return sum(of.get("quantite_produite", 0) for of in ofs if of.get("statut") == "termine")

def get_kpi_uph(tables):
    logs = tables.get("production_log", [])
    total_pieces = sum(pl.get("pieces_ok", 0) for pl in logs)
    total_heures = sum(
        (datetime.fromisoformat(pl["heure_fin"]) - datetime.fromisoformat(pl["heure_debut"])).total_seconds() / 3600
        for pl in logs
    )
    return round(total_pieces / total_heures, 2) if total_heures > 0 else None

def get_kpi_temps_cycle(tables):
    logs = tables.get("production_log", [])
    total_duree = sum(
        (datetime.fromisoformat(pl["heure_fin"]) - datetime.fromisoformat(pl["heure_debut"])).total_seconds()
        for pl in logs
    )
    total_pieces = sum(pl.get("pieces_ok", 0) for pl in logs)
    return round(total_duree / total_pieces, 2) if total_pieces > 0 else None

def get_kpi_taux_conformite(tables):
    controles = tables.get("controle_qualite", [])
    total_test = sum(c.get("quantite_testee", 0) for c in controles)
    total_ok = sum(c.get("quantite_conforme", 0) for c in controles)
    return round(100 * total_ok / total_test, 2) if total_test > 0 else None


def get_kpi_taux_defauts(tables):
    logs = tables.get("production_log", [])
    defauts = sum(pl.get("pieces_defectueuses", 0) for pl in logs)
    total = sum(pl.get("pieces_ok", 0) + pl.get("pieces_defectueuses", 0) for pl in logs)
    return round(100 * defauts / total, 2) if total > 0 else None


def get_kpi_taux_retouche(tables):
    logs = tables.get("production_log", [])
    retouches = sum(pl.get("pieces_retouchees", 0) for pl in logs)
    total = sum(pl.get("pieces_ok", 0) + pl.get("pieces_defectueuses", 0) for pl in logs)
    return round(100 * retouches / total, 2) if total > 0 else None


def get_kpi_lead_time(tables):
    ofs = tables.get("ordre_fabrication", [])
    lead_times = []
    for of in ofs:
        if of.get("statut") == "termine" and of.get("date_fin_reelle") and of.get("date_lancement"):
            debut = datetime.fromisoformat(of["date_lancement"])
            fin = datetime.fromisoformat(of["date_fin_reelle"])
            lead_times.append((fin - debut).total_seconds() / 3600)
    return round(sum(lead_times) / len(lead_times), 2) if lead_times else None


def get_kpi_respect_planning(tables):
    ofs = tables.get("ordre_fabrication", [])
    total, ok = 0, 0
    for of in ofs:
        if of.get("statut") == "termine" and of.get("date_fin_prevue") and of.get("date_fin_reelle"):
            total += 1
            if datetime.fromisoformat(of["date_fin_reelle"]) <= datetime.fromisoformat(of["date_fin_prevue"]):
                ok += 1
    return round(100 * ok / total, 2) if total > 0 else None


def get_kpi_adherence_plan(tables):
    ofs = tables.get("ordre_fabrication", [])
    total_prod = sum(of.get("quantite_produite", 0) for of in ofs)
    total_plan = sum(of.get("quantite_planifiee", 0) for of in ofs)
    return round(100 * total_prod / total_plan, 2) if total_plan > 0 else None


def get_kpi_wip(tables):
    ofs = tables.get("ordre_fabrication", [])
    return sum(of.get("quantite_planifiee", 0) - of.get("quantite_produite", 0) for of in ofs if of.get("statut") == "en_cours")

def get_kpi_trs(tables):
    logs = tables.get("production_log", [])
    machines = {m["id"]: m for m in tables.get("machine", [])}

    dispo_num, dispo_den, perf_num, perf_den, qual_num, qual_den = 0, 0, 0, 0, 0, 0

    for pl in logs:
        machine = machines.get(pl["machine_id"])
        if not machine:
            continue

        # durée réelle de production
        duree = (
            datetime.fromisoformat(pl["heure_fin"]) 
            - datetime.fromisoformat(pl["heure_debut"])
        ).total_seconds() / 3600

        # conversion inline pour temps_arret
        arret_val = pl.get("temps_arret", 0)
        try:
            arret = float(arret_val) if arret_val is not None else 0.0
        except:
            arret = 0.0

        dispo_num += max(duree - arret, 0)

        # conversion inline pour disponibilite_planifiee
        dispo_val = machine.get("disponibilite_planifiee", 0)
        try:
            dispo_den += float(dispo_val) if dispo_val is not None else 0.0
        except:
            dispo_den += 0.0

        prod_total = pl.get("pieces_ok", 0) + pl.get("pieces_defectueuses", 0)

        # conversion inline pour cadence_nominale
        cad_val = machine.get("cadence_nominale", 0)
        try:
            perf_den += (float(cad_val) if cad_val is not None else 0.0) * duree
        except:
            perf_den += 0.0

        perf_num += prod_total

        qual_num += pl.get("pieces_ok", 0)
        qual_den += prod_total

    dispo = dispo_num / dispo_den if dispo_den else 0
    perf = perf_num / perf_den if perf_den else 0
    qual = qual_num / qual_den if qual_den else 0

    return round(dispo * perf * qual * 100, 2)
