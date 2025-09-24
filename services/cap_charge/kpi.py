from datetime import datetime

# 1️⃣ Productivité : Volume traité / Heures travaillées
def get_kpi_productivite(tables):
    prod_rh = tables.get("production_rh", [])
    temps = tables.get("temps_travail", [])

    total_volume = sum(p["volume_traite"] for p in prod_rh)
    total_heures = 0
    for t in temps:
        debut = t["heure_debut"]
        fin = t["heure_fin"]
        if isinstance(debut, str):
            debut = datetime.fromisoformat(debut)
        if isinstance(fin, str):
            fin = datetime.fromisoformat(fin)
        total_heures += (fin - debut).total_seconds() / 3600
    return round(total_volume / total_heures, 2) if total_heures > 0 else None

# 2️⃣ Écart charge / capacité : Charge planifiée - Capacité disponible
def get_kpi_ecart_charge(tables):
    prod_rh = tables.get("production_rh", [])
    temps = tables.get("temps_travail", [])

    charge_planifie = sum(p["volume_planifie"] for p in prod_rh)
    capacite_disponible = 0
    for t in temps:
        debut = t["heure_debut"]
        fin = t["heure_fin"]
        if isinstance(debut, str):
            debut = datetime.fromisoformat(debut)
        if isinstance(fin, str):
            fin = datetime.fromisoformat(fin)
        capacite_disponible += (fin - debut).total_seconds() / 3600
    return round(charge_planifie - capacite_disponible, 2)

# 3️⃣ Taux d’utilisation RH : Temps productif / Temps total
def get_kpi_taux_utilisation(tables):
    temps = tables.get("temps_travail", [])
    total_productif = 0
    total_present = 0
    for t in temps:
        productif = t.get("temps_productif", 0)
        total = t.get("temps_total", 0)
        # si interval sous forme string
        if isinstance(productif, str):
            h, m, s = map(int, productif.split(":"))
            productif = h + m/60 + s/3600
        if isinstance(total, str):
            h, m, s = map(int, total.split(":"))
            total = h + m/60 + s/3600
        total_productif += productif
        total_present += total
    return round(100 * total_productif / total_present, 2) if total_present > 0 else None

# 4️⃣ Efficacité : Volume traité / Volume planifié
def get_kpi_efficacite(tables):
    prod_rh = tables.get("production_rh", [])
    total_traite = sum(p["volume_traite"] for p in prod_rh)
    total_planifie = sum(p["volume_planifie"] for p in prod_rh)
    return round(100 * total_traite / total_planifie, 2) if total_planifie > 0 else None

# 5️⃣ Coût horaire moyen par unité traitée
def get_kpi_cout_horaire_unite(tables):
    prod_rh = tables.get("production_rh", [])
    temps = tables.get("temps_travail", [])
    employes = {e["id"]: e for e in tables.get("employe", [])}

    total_cout = 0
    for t in temps:
        emp = employes.get(t["employe_id"])
        if not emp:
            continue
        debut = t["heure_debut"]
        fin = t["heure_fin"]
        if isinstance(debut, str):
            debut = datetime.fromisoformat(debut)
        if isinstance(fin, str):
            fin = datetime.fromisoformat(fin)
        heures = (fin - debut).total_seconds() / 3600
        total_cout += heures * emp.get("cout_horaire", 0)

    total_volume = sum(p["volume_traite"] for p in prod_rh)
    return round(total_cout / total_volume, 2) if total_volume > 0 else None

# 6️⃣ Taux d’erreur : Nb erreurs / Nb unités traitées
def get_kpi_taux_erreur(tables):
    prod_rh = tables.get("production_rh", [])
    total_erreurs = sum(p.get("erreurs", 0) for p in prod_rh)
    total_volume = sum(p["volume_traite"] for p in prod_rh)
    return round(100 * total_erreurs / total_volume, 2) if total_volume > 0 else None

# 7️⃣ Taux de recyclage : Déchets recyclés / Total déchets
def get_kpi_taux_recyclage(tables):
    dechets = tables.get("dechet", [])
    total_recycle = sum(d["dechet_recycle"] for d in dechets)
    total = sum(d["total_dechet"] for d in dechets)
    return round(100 * total_recycle / total, 2) if total > 0 else None
