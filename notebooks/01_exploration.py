# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Exploration des données de ponctualité
# MAGIC
# MAGIC Ce notebook lit les fichiers mensuels d'Infrabel déposés dans le volume, sans rien écrire. Il sert à décider comment construire le bronze et le silver :
# MAGIC
# MAGIC 1. les fichiers, leur séparateur et leurs colonnes ;
# MAGIC 2. le profil de chaque colonne (valeurs vides, valeurs distinctes, plus petite et plus grande valeur) et l'encodage ;
# MAGIC 3. le format des dates et des heures ;
# MAGIC 4. les contrôles de cohérence : retards recalculés, doublons, passages de minuit, valeurs extrêmes ;
# MAGIC 5. la distribution des retards et un premier aperçu des questions métier.
# MAGIC
# MAGIC Tout le code utilise l'API DataFrame de PySpark : il tourne sur le calcul serverless de la version gratuite, qui n'accepte ni les RDD ni `sparkContext`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paramètres
# MAGIC
# MAGIC Le notebook lit tous les CSV du volume `/Volumes/<catalogue>/<schéma>/<volume>`. Dans la version gratuite, le catalogue par défaut s'appelle en général `workspace` ; s'il porte un autre nom, il suffit de corriger le widget en haut du notebook. Le widget `encodage` reste à `UTF-8`, sauf si la section 2 signale des caractères mal décodés.

# COMMAND ----------

from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalogue", "workspace")
dbutils.widgets.text("schema", "bronze")
dbutils.widgets.text("volume", "fichiers")
dbutils.widgets.text("encodage", "UTF-8")
dbutils.widgets.text("seuil_retard_min", "6")
dbutils.widgets.text("passages_min_par_gare", "500")

CATALOGUE = dbutils.widgets.get("catalogue")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")
ENCODAGE = dbutils.widgets.get("encodage")
SEUIL_MIN = int(dbutils.widgets.get("seuil_retard_min"))
PASSAGES_MIN = int(dbutils.widgets.get("passages_min_par_gare"))
CHEMIN = f"/Volumes/{CATALOGUE}/{SCHEMA}/{VOLUME}"

# Les heures d'Infrabel sont des heures locales belges. On les lit sans conversion, comme si c'était de l'UTC :
# l'heure affichée reste celle de l'horaire, et l'heure qui n'existe pas la nuit du passage à l'heure d'été ne pose pas de problème.
# C'est déjà le réglage par défaut du calcul serverless ; on le fixe quand même pour ne dépendre d'aucun réglage.
spark.conf.set("spark.sql.session.timeZone", "Etc/UTC")

print(f"Catalogue courant : {spark.sql('SELECT current_catalog()').first()[0]}")
print(f"Dossier lu : {CHEMIN}")
print(f"Un train est à l'heure s'il a moins de {SEUIL_MIN} minutes de retard (règle d'Infrabel).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Les fichiers, le séparateur et les colonnes

# COMMAND ----------

fichiers = sorted(
    [f for f in dbutils.fs.ls(CHEMIN) if f.name.lower().endswith(".csv")],
    key=lambda f: f.name,
)
if not fichiers:
    raise ValueError(f"Aucun fichier CSV dans {CHEMIN}. Dépose d'abord les fichiers mensuels dans le volume.")

for f in fichiers:
    print(f"{f.name:<60} {f.size / 1024 / 1024:>8.1f} Mo")
print(f"\n{len(fichiers)} fichier(s), {sum(f.size for f in fichiers) / 1024 / 1024:.1f} Mo au total")

# COMMAND ----------

# Le séparateur se lit sur la première ligne du premier fichier : on compte les ';' et les ','.
premiere_ligne = spark.read.text(fichiers[0].path).first()[0]
SEPARATEUR = ";" if premiere_ligne.count(";") > premiere_ligne.count(",") else ","
print(f"En-tête : {premiere_ligne[:250]}")
print(f"Séparateur retenu : '{SEPARATEUR}'")

# COMMAND ----------

# Tout est lu en texte : les types se décident après avoir regardé les valeurs.
brut = (
    spark.read
    .option("header", True)
    .option("sep", SEPARATEUR)
    .option("inferSchema", False)
    .option("encoding", ENCODAGE)
    .option("pathGlobFilter", "*.[cC][sS][vV]")
    .csv(CHEMIN)
    .withColumn("_fichier", F.col("_metadata.file_name"))
)
# Noms de colonnes nettoyés : espaces et casse. Spark retire déjà la marque d'ordre des octets (BOM)
# en tête de fichier ; on la retire quand même du nom des colonnes, au cas où.
brut = brut.toDF(*[c if c == "_fichier" else c.replace("\ufeff", "").strip().upper() for c in brut.columns])

ATTENDUES = [
    "DATDEP", "TRAIN_NO", "RELATION", "TRAIN_SERV", "PTCAR_NO", "LINE_NO_DEP",
    "REAL_TIME_ARR", "REAL_TIME_DEP", "PLANNED_TIME_ARR", "PLANNED_TIME_DEP",
    "DELAY_ARR", "DELAY_DEP", "RELATION_DIRECTION", "PTCAR_LG_NM_NL", "LINE_NO_ARR",
    "PLANNED_DATE_ARR", "PLANNED_DATE_DEP", "REAL_DATE_ARR", "REAL_DATE_DEP",
]
colonnes = [c for c in brut.columns if c != "_fichier"]
manquantes = [c for c in ATTENDUES if c not in colonnes]
en_plus = [c for c in colonnes if c not in ATTENDUES]

print(f"{len(colonnes)} colonnes lues.")
print("Colonnes attendues absentes :", ", ".join(manquantes) if manquantes else "aucune")
print("Colonnes en plus :", ", ".join(en_plus) if en_plus else "aucune")
if manquantes:
    print("\nAttention : la suite du notebook suppose les colonnes de la documentation Infrabel. Adapte les noms avant de continuer.")

# COMMAND ----------

TOTAL = brut.count()
print(f"{TOTAL:,} lignes au total".replace(",", " "))
display(brut.groupBy("_fichier").count().orderBy("_fichier"))

# COMMAND ----------

display(brut.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Le profil des colonnes
# MAGIC
# MAGIC Une seule lecture des données pour toutes les colonnes : le nombre de valeurs vides, une estimation du nombre de valeurs distinctes, et la plus petite et la plus grande valeur en texte. Ces deux dernières suffisent souvent à voir le format d'une colonne.
# MAGIC
# MAGIC La même lecture compte les lignes dont un texte contient « � » : ce caractère apparaît quand un fichier qui n'est pas en UTF-8 est lu comme tel.

# COMMAND ----------

def est_vide(colonne):
    return F.col(colonne).isNull() | (F.trim(F.col(colonne)) == "")


COLONNES_TEXTE = [c for c in ["RELATION", "RELATION_DIRECTION", "PTCAR_LG_NM_NL"] if c in colonnes]
mal_decodee = reduce(
    lambda a, b: a | b,
    [F.coalesce(F.col(c).contains("\ufffd"), F.lit(False)) for c in COLONNES_TEXTE],
    F.lit(False),
)

mesures = [F.sum(mal_decodee.cast("int")).alias("__mal_decodees")]
for c in colonnes:
    mesures += [
        F.sum(est_vide(c).cast("int")).alias(f"{c}__vides"),
        F.approx_count_distinct(c).alias(f"{c}__distinctes"),
        F.min(c).alias(f"{c}__min"),
        F.max(c).alias(f"{c}__max"),
    ]
r = brut.agg(*mesures).first().asDict()

MAL_DECODEES = int(r["__mal_decodees"] or 0)
print(f"Lignes avec des caractères mal décodés : {MAL_DECODEES}")
if MAL_DECODEES:
    print(f"Les fichiers ne sont sans doute pas en {ENCODAGE} : mettre ISO-8859-1 dans le widget « encodage » et relancer le notebook.")

profil = spark.createDataFrame(
    [
        (
            c,
            int(r[f"{c}__vides"] or 0),
            round(100.0 * (r[f"{c}__vides"] or 0) / TOTAL, 2) if TOTAL else None,
            int(r[f"{c}__distinctes"] or 0),
            r[f"{c}__min"],
            r[f"{c}__max"],
        )
        for c in colonnes
    ],
    "colonne string, vides long, pct_vides double, distinctes_approx long, min_texte string, max_texte string",
)
display(profil)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Le format des dates et des heures
# MAGIC
# MAGIC Pour chaque colonne de date ou d'heure, on essaie plusieurs formats et on garde celui qui reconnaît le plus de valeurs. `try_to_timestamp` renvoie `NULL` au lieu de lever une erreur : c'est indispensable avec le mode ANSI, actif par défaut sur le calcul serverless.

# COMMAND ----------

COLONNES_DATE = [c for c in ["DATDEP", "PLANNED_DATE_ARR", "PLANNED_DATE_DEP", "REAL_DATE_ARR", "REAL_DATE_DEP"] if c in colonnes]
COLONNES_HEURE = [c for c in ["PLANNED_TIME_ARR", "PLANNED_TIME_DEP", "REAL_TIME_ARR", "REAL_TIME_DEP"] if c in colonnes]
FORMATS_DATE = ["yyyy-MM-dd", "dd/MM/yyyy", "dd-MM-yyyy", "ddMMMyyyy", "dd-MMM-yyyy", "dd-MMM-yy", "yyyyMMdd"]
FORMATS_HEURE = ["HH:mm:ss", "H:mm:ss", "HH:mm", "HHmmss"]


def meilleurs_formats(liste_colonnes, formats):
    """Pour chaque colonne, la part des valeurs remplies reconnue par chaque format, en une seule lecture."""
    exprs = []
    for c in liste_colonnes:
        exprs.append(F.sum((~est_vide(c)).cast("int")).alias(f"{c}__remplies"))
        for i, fmt in enumerate(formats):
            reconnue = F.try_to_timestamp(F.trim(F.col(c)), F.lit(fmt)).isNotNull()
            exprs.append(F.sum(reconnue.cast("int")).alias(f"{c}__{i}"))
    res = brut.agg(*exprs).first().asDict()
    lignes, choix = [], {}
    for c in liste_colonnes:
        remplies = res[f"{c}__remplies"] or 0
        scores = [(fmt, (res[f"{c}__{i}"] or 0) / remplies if remplies else 0.0) for i, fmt in enumerate(formats)]
        fmt, taux = max(scores, key=lambda s: s[1])
        choix[c] = fmt if taux > 0 else None
        lignes.append((c, int(remplies), choix[c], round(100.0 * taux, 2)))
    return lignes, choix


lignes_dates, FORMATS = meilleurs_formats(COLONNES_DATE, FORMATS_DATE)
lignes_heures, formats_heures = meilleurs_formats(COLONNES_HEURE, FORMATS_HEURE)
FORMATS.update(formats_heures)

display(spark.createDataFrame(
    lignes_dates + lignes_heures,
    "colonne string, valeurs_remplies long, format_retenu string, pct_reconnues double",
))
sans_format = [c for c, fmt in FORMATS.items() if fmt is None]
if sans_format:
    print("Aucun format reconnu pour :", ", ".join(sans_format), ". Voir leurs valeurs dans le profil et ajouter le bon format à la liste.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Une version typée, pour les contrôles
# MAGIC
# MAGIC Les horodatages assemblent la date et l'heure de chaque événement : un train de nuit peut arriver le lendemain de son départ. Les retards passent par un `try_cast` en nombre : une valeur qui n'est pas un nombre devient `NULL`, et on la compte.

# COMMAND ----------

def horodatage(col_date, col_heure):
    fmt_date, fmt_heure = FORMATS.get(col_date), FORMATS.get(col_heure)
    if fmt_date is None or fmt_heure is None:
        return F.lit(None).cast("timestamp")
    texte = F.concat_ws(" ", F.trim(F.col(col_date)), F.trim(F.col(col_heure)))
    return F.try_to_timestamp(texte, F.lit(f"{fmt_date} {fmt_heure}"))


def en_secondes(colonne):
    # Deux try_cast : un texte comme « 120.0 » passe par un décimal, et rien ne lève d'erreur en mode ANSI.
    return F.expr(f"try_cast(try_cast(trim({colonne}) AS DOUBLE) AS INT)")


typee = brut.select(
    "_fichier",
    F.try_to_timestamp(F.trim(F.col("DATDEP")), F.lit(FORMATS.get("DATDEP") or "yyyy-MM-dd")).cast("date").alias("date_depart"),
    F.trim("TRAIN_NO").alias("train_no"),
    F.trim("RELATION").alias("relation"),
    F.trim("TRAIN_SERV").alias("operateur"),
    F.trim("PTCAR_NO").alias("ptcar_no"),
    F.trim("PTCAR_LG_NM_NL").alias("point_arret"),
    horodatage("PLANNED_DATE_ARR", "PLANNED_TIME_ARR").alias("arrivee_prevue"),
    horodatage("REAL_DATE_ARR", "REAL_TIME_ARR").alias("arrivee_reelle"),
    horodatage("PLANNED_DATE_DEP", "PLANNED_TIME_DEP").alias("depart_prevu"),
    horodatage("REAL_DATE_DEP", "REAL_TIME_DEP").alias("depart_reel"),
    en_secondes("DELAY_ARR").alias("retard_arr_s"),
    en_secondes("DELAY_DEP").alias("retard_dep_s"),
    (~est_vide("DELAY_ARR") & F.expr("try_cast(trim(DELAY_ARR) AS DOUBLE)").isNull()).alias("retard_arr_illisible"),
    (~est_vide("DELAY_DEP") & F.expr("try_cast(trim(DELAY_DEP) AS DOUBLE)").isNull()).alias("retard_dep_illisible"),
).withColumn("retard_arr_min", F.col("retard_arr_s") / 60.0)

display(typee.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Les contrôles de cohérence

# COMMAND ----------

# 5.1 Le retard publié correspond-il à l'écart entre l'heure réelle et l'heure prévue ?
ecart = (F.unix_timestamp("arrivee_reelle") - F.unix_timestamp("arrivee_prevue")) - F.col("retard_arr_s")
coherence = (
    typee.where(F.col("arrivee_reelle").isNotNull() & F.col("arrivee_prevue").isNotNull() & F.col("retard_arr_s").isNotNull())
    .agg(
        F.count(F.lit(1)).alias("arrivees_comparables"),
        F.sum((F.abs(ecart) <= 60).cast("int")).alias("ecart_max_1_minute"),
        F.sum((F.abs(ecart) > 60).cast("int")).alias("ecart_plus_1_minute"),
    )
)
display(coherence)

# COMMAND ----------

# 5.2 Les champs vides attendus : pas d'arrivée au départ du train, pas de départ au terminus.
display(typee.agg(
    F.sum(F.col("arrivee_prevue").isNull().cast("int")).alias("sans_arrivee_prevue"),
    F.sum(F.col("depart_prevu").isNull().cast("int")).alias("sans_depart_prevu"),
    F.sum((F.col("arrivee_prevue").isNull() & F.col("depart_prevu").isNull()).cast("int")).alias("ni_arrivee_ni_depart"),
    F.sum(F.col("retard_arr_illisible").cast("int")).alias("retard_arr_non_numerique"),
    F.sum(F.col("retard_dep_illisible").cast("int")).alias("retard_dep_non_numerique"),
    F.sum(F.col("date_depart").isNull().cast("int")).alias("date_depart_illisible"),
))

# COMMAND ----------

# 5.3 Les doublons : sur la ligne entière, puis sur la clé « un train, un jour, un point d'arrêt, une heure prévue ».
colonnes_source = [c for c in brut.columns if c != "_fichier"]
doublons_exacts = TOTAL - brut.select(*colonnes_source).distinct().count()

cle = ["date_depart", "train_no", "ptcar_no", "arrivee_prevue", "depart_prevu"]
doublons_cle = (
    typee.groupBy(*cle).count().where(F.col("count") > 1)
    .agg(F.count(F.lit(1)).alias("cles_en_double"), F.sum(F.col("count") - 1).alias("lignes_en_trop"))
)
print(f"Lignes identiques en double : {doublons_exacts}")
display(doublons_cle)

# Un même train peut-il passer deux fois au même point le même jour ? Utile pour choisir la clé du silver.
display(
    typee.groupBy("date_depart", "train_no", "ptcar_no").count().where(F.col("count") > 1)
    .agg(F.count(F.lit(1)).alias("train_jour_point_multiples"))
)

# COMMAND ----------

# 5.4 Les passages de minuit et les retards extrêmes.
display(typee.agg(
    F.sum((F.to_date("arrivee_prevue") > F.col("date_depart")).cast("int")).alias("arrivees_le_lendemain"),
    F.sum((F.col("retard_arr_s") > 3 * 3600).cast("int")).alias("retards_plus_de_3h"),
    F.sum((F.col("retard_arr_s") < -30 * 60).cast("int")).alias("avances_plus_de_30_min"),
    F.min("retard_arr_s").alias("retard_min_s"),
    F.max("retard_arr_s").alias("retard_max_s"),
))
display(typee.where(F.col("retard_arr_s") > 3 * 3600).orderBy(F.desc("retard_arr_s")).limit(20))

# COMMAND ----------

# 5.5 Un numéro de point d'arrêt a-t-il toujours le même nom ? Ça dira si la dimension gare peut s'appuyer sur PTCAR_NO.
noms_par_point = typee.groupBy("ptcar_no").agg(F.countDistinct("point_arret").alias("noms"), F.collect_set("point_arret").alias("variantes"))
print(f"Points d'arrêt distincts : {noms_par_point.count()}")
display(noms_par_point.where("noms > 1").orderBy(F.desc("noms")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. La distribution des retards à l'arrivée

# COMMAND ----------

arrivees = typee.where(F.col("retard_arr_s").isNotNull())
display(arrivees.agg(
    F.count(F.lit(1)).alias("arrivees_mesurees"),
    F.round(F.avg("retard_arr_min"), 2).alias("retard_moyen_min"),
    F.percentile_approx("retard_arr_min", [0.5, 0.75, 0.9, 0.95, 0.99]).alias("p50_p75_p90_p95_p99_min"),
    F.round(100 * F.avg((F.col("retard_arr_min") < SEUIL_MIN).cast("double")), 2).alias("pct_moins_du_seuil"),
))

tranche = (
    F.when(F.col("retard_arr_min") < 0, "1. en avance")
    .when(F.col("retard_arr_min") < 1, "2. moins d'1 min")
    .when(F.col("retard_arr_min") < 3, "3. 1 à 3 min")
    .when(F.col("retard_arr_min") < SEUIL_MIN, f"4. 3 à {SEUIL_MIN} min")
    .when(F.col("retard_arr_min") < 15, f"5. {SEUIL_MIN} à 15 min")
    .when(F.col("retard_arr_min") < 30, "6. 15 à 30 min")
    .otherwise("7. 30 min et plus")
)
display(arrivees.groupBy(tranche.alias("tranche")).count().orderBy("tranche"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Un premier aperçu des questions métier
# MAGIC
# MAGIC Ces chiffres ne sont pas encore le gold : ils servent à vérifier que les données répondent bien aux questions du projet. Dans Databricks, le bouton `+` au-dessus d'un résultat le transforme en graphique.

# COMMAND ----------

# 7.1 La ponctualité au terminus, comme Infrabel la mesure : le dernier arrêt de chaque train, chaque jour.
dernier_arret = Window.partitionBy("date_depart", "train_no").orderBy(F.col("arrivee_prevue").desc_nulls_last())
terminus = (
    typee.where(F.col("arrivee_prevue").isNotNull())
    .withColumn("rang", F.row_number().over(dernier_arret))
    .where("rang = 1")
    .where(F.col("retard_arr_s").isNotNull())
)
display(terminus.agg(
    F.count(F.lit(1)).alias("trains_arrives"),
    F.round(100 * F.avg((F.col("retard_arr_min") < SEUIL_MIN).cast("double")), 2).alias("pct_a_l_heure_au_terminus"),
))
display(
    terminus.groupBy(F.date_format("date_depart", "yyyy-MM").alias("mois"))
    .agg(F.count(F.lit(1)).alias("trains"),
         F.round(100 * F.avg((F.col("retard_arr_min") < SEUIL_MIN).cast("double")), 2).alias("pct_a_l_heure"))
    .orderBy("mois")
)

# COMMAND ----------

# 7.2 Selon l'heure d'arrivée prévue : les pointes du matin et du soir se voient-elles ?
display(
    arrivees.groupBy(F.hour("arrivee_prevue").alias("heure"))
    .agg(F.count(F.lit(1)).alias("arrivees"),
         F.round(F.avg("retard_arr_min"), 2).alias("retard_moyen_min"),
         F.round(100 * F.avg((F.col("retard_arr_min") < SEUIL_MIN).cast("double")), 2).alias("pct_a_l_heure"))
    .orderBy("heure")
)

# COMMAND ----------

# 7.3 Selon le jour de la semaine. Spark numérote les jours de 1 (dimanche) à 7 (samedi) ; les noms sont mis en français.
NOMS_DES_JOURS = F.array(*[F.lit(j) for j in ["dimanche", "lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi"]])
display(
    arrivees.groupBy(F.dayofweek("date_depart").alias("jour"), F.element_at(NOMS_DES_JOURS, F.dayofweek("date_depart")).alias("nom_du_jour"))
    .agg(F.count(F.lit(1)).alias("arrivees"),
         F.round(F.avg("retard_arr_min"), 2).alias("retard_moyen_min"))
    .orderBy("jour")
)

# COMMAND ----------

# 7.4 Les points d'arrêt où l'on arrive le plus en retard, parmi ceux qui voient assez de trains.
display(
    arrivees.groupBy("ptcar_no", "point_arret")
    .agg(F.count(F.lit(1)).alias("arrivees"),
         F.round(F.avg("retard_arr_min"), 2).alias("retard_moyen_min"),
         F.round(100 * F.avg((F.col("retard_arr_min") < SEUIL_MIN).cast("double")), 2).alias("pct_a_l_heure"))
    .where(F.col("arrivees") >= PASSAGES_MIN)
    .orderBy(F.desc("retard_moyen_min"))
    .limit(15)
)

# COMMAND ----------

# 7.5 Le retard se propage-t-il le long du trajet ? Écart de retard entre deux arrêts successifs d'un même train.
ordre_arrets = Window.partitionBy("date_depart", "train_no").orderBy(F.col("arrivee_prevue").asc_nulls_first())
propagation = (
    arrivees.withColumn("retard_arret_precedent", F.lag("retard_arr_min").over(ordre_arrets))
    .withColumn("rang_arret", F.row_number().over(ordre_arrets))
    .where(F.col("retard_arret_precedent").isNotNull())
)
display(
    propagation.groupBy(F.least(F.col("rang_arret"), F.lit(10)).alias("rang_arret_plafonne_a_10"))
    .agg(F.round(F.avg("retard_arr_min"), 2).alias("retard_moyen_min"),
         F.round(F.avg(F.col("retard_arr_min") - F.col("retard_arret_precedent")), 2).alias("retard_gagne_depuis_l_arret_precedent"))
    .orderBy("rang_arret_plafonne_a_10")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Ce que l'exploration a montré
# MAGIC
# MAGIC Résultats sur août et septembre 2026 (4 051 748 lignes), et ce qu'ils décident pour la suite :
# MAGIC
# MAGIC - **Séparateur et encodage** : détectés sur l'en-tête, UTF-8 sans aucun caractère mal décodé. Le bronze reprend la même détection.
# MAGIC - **Colonnes** : les 19 de la documentation, plus `CIRC_TYP` (toujours « 1 »), `OP1_COD` et `THOP1_COD` (8 codes, vides au premier et au dernier arrêt). Le bronze garde tout ; le silver décidera quoi en faire.
# MAGIC - **Formats** : dates `ddMMMyyyy` (01AUG2026) et heures `H:mm:ss` sans zéro devant (9:59:59), reconnus à 100 %.
# MAGIC - **Passages de minuit** : 20 206 arrivées tombent le lendemain du départ. Les horodatages doivent assembler la date et l'heure.
# MAGIC - **Clé du silver** : aucun doublon exact, mais 47 clés en double avec un contenu différent, et 98 cas où un train passe deux fois au même point le même jour. La clé inclut donc les heures prévues ; la règle pour les 47 cas dépend de la section 9.
# MAGIC - **Retards** : aucun n'est illisible. 404 dépassent 3 h, dont le record de 11 h 27 pour l'European Sleeper n° 474 du 4 août, en retard sur tout son trajet : on les garde. Les 19 avances de plus de 30 min sont suspectes.
# MAGIC - **Cohérence** : pour 4 944 arrivées (0,13 %), le retard publié s'écarte de plus d'une minute des horaires. La section 9 dit de combien.
# MAGIC - **Dimension gare** : 642 points d'arrêt, chacun avec un seul nom. `PTCAR_NO` sert de clé.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Deux zooms pour le silver
# MAGIC
# MAGIC Le premier montre ce qui change entre les lignes d'une même clé en double. Le second montre de combien les retards incohérents s'écartent des horaires, et chez quel opérateur.

# COMMAND ----------

# 9.1 Les clés en double, ligne à ligne.
cle_brute = ["DATDEP", "TRAIN_NO", "PTCAR_NO", "PLANNED_DATE_ARR", "PLANNED_TIME_ARR", "PLANNED_DATE_DEP", "PLANNED_TIME_DEP"]
fenetre = Window.partitionBy(*cle_brute)
display(brut.withColumn("n", F.count(F.lit(1)).over(fenetre)).where("n > 1").orderBy(*cle_brute).limit(40))

# COMMAND ----------

# 9.2 L'écart, en minutes, entre le retard publié et le retard recalculé sur les horaires.
ecart_s = (F.unix_timestamp("arrivee_reelle") - F.unix_timestamp("arrivee_prevue")) - F.col("retard_arr_s")
display(
    typee.withColumn("ecart_s", ecart_s).where(F.abs("ecart_s") > 60)
    .groupBy("operateur", F.round(F.col("ecart_s") / 60).cast("int").alias("ecart_min"))
    .count().orderBy(F.desc("count")).limit(20)
)
