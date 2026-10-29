"""Teste le projet dbt sur un Spark local, sans Databricks : c'est ce que lance GitHub Actions à chaque pull request.

1. Un silver d'exemple, synthétique et reproductible : six jours d'août 2026 (dont le 15 août, un samedi férié),
   douze points du réseau dont la jonction Nord-Midi, des trains IC, L, S, P et un Eurostar venu de Paris,
   un train de nuit qui arrive après minuit.
2. `dbt build` sur ce silver : le seed, les 13 modèles, les tests de données et les tests unitaires.

Usage : python ci/tester_dbt.py
"""
import hashlib
import os
import random
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession

# Les processus Python de Spark doivent tourner avec le même interpréteur que ce script.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

PROJET = Path(__file__).resolve().parent.parent / "dbt"

JOURS = [date(2026, 8, 13) + timedelta(days=i) for i in range(6)]
POINTS = {101: "BRUSSEL-NOORD", 102: "BRUSSEL-CENTRAAL", 103: "BRUSSEL-ZUID", 104: "MECHELEN",
          105: "ANTWERPEN-CENTRAAL", 106: "LEUVEN", 107: "NAMUR", 108: "LIEGE-GUILLEMINS", 109: "ANS",
          110: "HERGENRATH", 111: "GENT-SINT-PIETERS", 112: "OTTIGNIES"}
LIGNES = [
    # relation, sens, opérateur, points du trajet, entre depuis l'étranger, heures de départ
    ("IC 01", "A", "SNCB/NMBS", [111, 103, 102, 101, 104, 105], False, [(7, 4), (12, 4), (17, 4)]),
    ("IC 16", "B", "SNCB/NMBS", [105, 104, 101, 102, 103, 112, 107, 108], False, [(6, 50), (16, 50), (23, 20)]),
    ("L 05", "A", "SNCB/NMBS", [108, 109, 110], False, [(8, 15), (18, 15)]),
    ("S1", "A", "SNCB/NMBS", [104, 106, 101, 102, 103], False, [(7, 30), (17, 30)]),
    ("P 7001", "A", "SNCB/NMBS", [107, 112, 103, 102, 101], False, [(7, 0)]),
    ("EURST", "A", "THI-FACT", [103, 102, 101, 108, 110], True, [(9, 10), (19, 10)]),
]
COLONNES = """id_passage STRING, date_depart DATE, train_no INT, relation STRING, sens_relation STRING, operateur STRING,
    ptcar_no INT, point_arret STRING, ligne_depart STRING, ligne_arrivee STRING, arrivee_prevue TIMESTAMP,
    arrivee_reelle TIMESTAMP, depart_prevu TIMESTAMP, depart_reel TIMESTAMP, retard_arrivee_s INT, retard_depart_s INT,
    retard_arrivee_min DOUBLE, retard_depart_min DOUBLE, retard_arrivee_incoherent BOOLEAN,
    retard_depart_incoherent BOOLEAN, circ_typ STRING, op1_cod STRING, thop1_cod STRING, _fichier STRING,
    _charge_le TIMESTAMP, _traite_le TIMESTAMP"""


def passages():
    """Les lignes du silver d'exemple, avec les mêmes colonnes que la table créée par 03_silver."""
    alea = random.Random(2026)
    lignes = []
    for jour in JOURS:
        for n, (relation, sens, operateur, trajet, etranger, departs) in enumerate(LIGNES):
            for k, (h, m) in enumerate(departs):
                train_no = 1000 * (n + 1) + 10 * k + jour.day % 10
                heure = datetime.combine(jour, datetime.min.time()) + timedelta(hours=h, minutes=m)
                retard = alea.uniform(0, 2)
                for rang, ptcar in enumerate(trajet):
                    premier, dernier = rang == 0, rang == len(trajet) - 1
                    arrivee = None if premier and not etranger else heure - timedelta(minutes=1)
                    depart = None if dernier else heure
                    if arrivee is not None and not premier:
                        retard += alea.gauss(0.6 if ptcar in (101, 102, 103) else 0.2, 1.5)
                    elif premier and etranger:
                        retard = alea.uniform(3, 12)   # le retard pris avant la frontière
                    retard = max(retard, -1.0)
                    ret_arr = None if arrivee is None else round(retard, 2)
                    ret_dep = None if depart is None else round(retard + max(alea.gauss(0.1, 0.3), 0), 2)
                    fmt = lambda t: None if t is None else t.strftime("%Y-%m-%d %H:%M:%S")  # noqa: E731
                    reel = lambda t, r: None if t is None else fmt(t + timedelta(minutes=r))  # noqa: E731
                    cle = f"{jour}|{train_no}|{ptcar}|{fmt(arrivee)}|{fmt(depart)}"
                    lignes.append((
                        hashlib.sha256(cle.encode()).hexdigest(), str(jour), train_no, relation, sens, operateur,
                        ptcar, POINTS[ptcar], None, None, fmt(arrivee), reel(arrivee, ret_arr), fmt(depart),
                        reel(depart, ret_dep), None if ret_arr is None else round(ret_arr * 60),
                        None if ret_dep is None else round(ret_dep * 60), ret_arr, ret_dep, False, False,
                        "COMMERCIAL", operateur, operateur, "exemple.csv", "2026-09-01 06:00:00", "2026-09-01 06:05:00"))
                    heure += timedelta(minutes=12)
    return lignes


def main():
    travail = Path(tempfile.mkdtemp(prefix="dbt-spark-"))
    spark = (SparkSession.builder.master("local[2]").appName("tests-dbt")
             .config("spark.sql.warehouse.dir", str(travail / "warehouse"))
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.ansi.enabled", "true")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    # Les heures arrivent en texte puis sont converties par Spark, en UTC comme dans le silver.
    brut = spark.createDataFrame(passages(), COLONNES.replace("TIMESTAMP", "STRING").replace("DATE,", "STRING,"))
    types = {c.split()[0]: c.split()[1] for c in (x.strip() for x in COLONNES.split(","))}
    silver = brut.selectExpr(*[f"CAST({c} AS {t}) AS {c}" for c, t in types.items()])
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    silver.write.mode("overwrite").saveAsTable("silver.passages")
    print(f"silver d'exemple : {silver.count()} passages, {silver.select('train_no', 'date_depart').distinct().count()} trains-jours")

    # dbt-spark (méthode session) reprend la session Spark déjà ouverte dans ce processus.
    from dbt.cli.main import dbtRunner

    resultat = dbtRunner().invoke(["build", "--project-dir", str(PROJET), "--profiles-dir", str(PROJET / "profiles"),
                                   "--target", "spark_local"])
    if resultat.success:
        print("\nPonctualité au terminus sur le silver d'exemple :")
        spark.table("gold.v_ponctualite_mois").show()
        spark.table("gold.v_jonction_nord_midi").show()
    spark.stop()
    return 0 if resultat.success else 1


if __name__ == "__main__":
    sys.exit(main())
