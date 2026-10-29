# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver : des passages propres et typés
# MAGIC
# MAGIC Ce notebook lit les lignes nouvelles du bronze, leur donne de vrais types, les contrôle, puis les range dans deux tables Delta :
# MAGIC
# MAGIC - `silver.passages` : un passage par train, par jour, par point d'arrêt et par horaire prévu, prêt pour le gold ;
# MAGIC - `silver.passages_quarantaine` : les lignes rejetées, avec les raisons du rejet, pour ne rien perdre.
# MAGIC
# MAGIC Le traitement est **incrémental** : le journal `silver.journal_traitements` garde l'heure du dernier chargement bronze traité, et chaque exécution ne lit que les lignes arrivées depuis. Les écritures passent par des `MERGE` sur une clé stable : relancer le notebook, même après une erreur au milieu, ne crée jamais de doublon.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Les règles, tirées de l'exploration
# MAGIC
# MAGIC | Constat sur août et septembre 2026 (notebook 01) | Règle du silver |
# MAGIC |---|---|
# MAGIC | Dates comme `01AUG2026`, heures comme `9:59:59` | Formats `ddMMMyyyy` et `H:mm:ss`, date et heure assemblées en un horodatage |
# MAGIC | 20 206 arrivées le lendemain du départ | L'horodatage garde le bon jour, même après minuit |
# MAGIC | 98 cas où un train passe deux fois au même point le même jour | La clé contient les heures prévues : date, train, point, arrivée prévue, départ prévu |
# MAGIC | 47 clés en double avec un contenu différent, dont un train entier en deux versions | On garde la ligne la plus complète, puis celle qui a un vrai retard mesuré ; les autres vont en quarantaine |
# MAGIC | 4 944 retards qui s'écartent des horaires, presque tous d'une ou deux minutes | On garde le retard publié par Infrabel et on pose un indicateur |
# MAGIC | 404 retards de plus de 3 h, de vrais retards | Gardés |
# MAGIC | 19 avances de plus de 30 min | Quarantaine |
# MAGIC | Aucune valeur illisible ni hors schéma | Quarantaine, si cela arrive un jour |
# MAGIC
# MAGIC Avant de charger des mois plus anciens, mieux vaut relancer le notebook 01 sur ces fichiers : leur format a pu changer, et des dates illisibles finiraient en quarantaine.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paramètres
# MAGIC
# MAGIC Le widget `reinitialiser` vide les tables du silver et le journal : tout le bronze est alors retraité. Après une réinitialisation du bronze, inutile d'y toucher : les lignes rechargées sont retraitées et le `MERGE` évite les doublons.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalogue", "workspace")
dbutils.widgets.text("schema_bronze", "bronze")
dbutils.widgets.text("schema_silver", "silver")
dbutils.widgets.dropdown("reinitialiser", "non", ["non", "oui"])

CATALOGUE = dbutils.widgets.get("catalogue")
SCHEMA_BRONZE = dbutils.widgets.get("schema_bronze")
SCHEMA_SILVER = dbutils.widgets.get("schema_silver")
REINITIALISER = dbutils.widgets.get("reinitialiser") == "oui"

BRONZE = f"{CATALOGUE}.{SCHEMA_BRONZE}.passages_bruts"
SILVER = f"{CATALOGUE}.{SCHEMA_SILVER}.passages"
QUARANTAINE = f"{CATALOGUE}.{SCHEMA_SILVER}.passages_quarantaine"
JOURNAL = f"{CATALOGUE}.{SCHEMA_SILVER}.journal_traitements"
LOT = f"{CATALOGUE}.{SCHEMA_SILVER}.lot_en_cours"

FORMAT_DATE = "ddMMMyyyy"                          # 01AUG2026
FORMAT_HEURE = "H:mm:ss"                           # 9:59:59, sans zéro devant
FORMAT_HORODATAGE = "yyyy-MM-dd HH:mm:ss.SSSSSS"   # pour garder une heure Spark en texte
AVANCE_MAX_S = 30 * 60                             # au-delà, une avance est suspecte
ECART_TOLERE_S = 60                                # écart toléré entre le retard publié et les horaires

# Comme dans le notebook 01 : les heures belges sont lues sans conversion de fuseau.
spark.conf.set("spark.sql.session.timeZone", "Etc/UTC")


def nombre(x):
    """Un entier écrit à la française, avec une espace pour les milliers."""
    return f"{int(x or 0):,}".replace(",", " ")

# COMMAND ----------

if REINITIALISER:
    for table in (SILVER, QUARANTAINE, JOURNAL, LOT):
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    print("Tables du silver et journal supprimés : tout le bronze va être retraité.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Les tables
# MAGIC
# MAGIC Elles sont créées avec un schéma explicite et des commentaires, visibles dans l'explorateur de catalogue. La quarantaine garde la ligne brute en JSON : sa structure ne dépend pas des colonnes du CSV, même si Infrabel en ajoute.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER} (
    id_passage STRING COMMENT 'Clé : empreinte SHA-256 de la date, du train, du point et des heures prévues',
    date_depart DATE COMMENT 'Date de départ du train',
    train_no INT COMMENT 'Numéro du train',
    relation STRING COMMENT 'Relation commerciale, par exemple IC 01',
    sens_relation STRING COMMENT 'Sens de la relation',
    operateur STRING COMMENT 'Opérateur ferroviaire',
    ptcar_no INT COMMENT 'Numéro du point (PTCAR)',
    point_arret STRING COMMENT 'Nom du point, en néerlandais',
    ligne_depart STRING COMMENT 'Ligne empruntée en quittant le point',
    ligne_arrivee STRING COMMENT 'Ligne empruntée pour arriver au point',
    arrivee_prevue TIMESTAMP COMMENT 'Arrivée prévue, heure belge',
    arrivee_reelle TIMESTAMP COMMENT 'Arrivée réelle, heure belge',
    depart_prevu TIMESTAMP COMMENT 'Départ prévu, heure belge',
    depart_reel TIMESTAMP COMMENT 'Départ réel, heure belge',
    retard_arrivee_s INT COMMENT 'Retard publié en arrivant au point, en secondes (négatif si le train est en avance)',
    retard_depart_s INT COMMENT 'Retard publié en quittant le point, en secondes (négatif si le train est en avance)',
    retard_arrivee_min DOUBLE COMMENT 'Retard publié en arrivant au point, en minutes',
    retard_depart_min DOUBLE COMMENT 'Retard publié en quittant le point, en minutes',
    retard_arrivee_incoherent BOOLEAN COMMENT 'Vrai si le retard publié et les heures diffèrent de plus de 60 secondes',
    retard_depart_incoherent BOOLEAN COMMENT 'Vrai si le retard publié et les heures diffèrent de plus de 60 secondes',
    circ_typ STRING COMMENT 'Colonne CIRC_TYP du CSV, non documentée',
    op1_cod STRING COMMENT 'Colonne OP1_COD du CSV, non documentée',
    thop1_cod STRING COMMENT 'Colonne THOP1_COD du CSV, non documentée',
    _fichier STRING COMMENT 'CSV source',
    _charge_le TIMESTAMP COMMENT 'Heure du chargement bronze',
    _traite_le TIMESTAMP COMMENT 'Heure du traitement silver'
)
COMMENT 'Passages des trains Infrabel, typés et contrôlés : une ligne par train, jour, point et horaire prévu'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUARANTAINE} (
    _id_ligne STRING COMMENT 'Empreinte SHA-256 de la ligne brute',
    raisons ARRAY<STRING> COMMENT 'Règles de qualité non respectées',
    id_passage STRING COMMENT 'Clé du passage, calculée avec les valeurs lisibles',
    ligne_brute STRING COMMENT 'La ligne du CSV, en JSON',
    _rescued_data STRING COMMENT 'Valeurs hors schéma relevées par Auto Loader',
    _fichier STRING COMMENT 'CSV source',
    _charge_le TIMESTAMP COMMENT 'Heure du chargement bronze',
    _mis_en_quarantaine_le TIMESTAMP COMMENT 'Heure de la mise en quarantaine'
)
COMMENT 'Lignes du bronze rejetées par les règles du silver, avec leurs raisons'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {JOURNAL} (
    traite_le TIMESTAMP COMMENT 'Heure du traitement',
    debut_bronze TIMESTAMP COMMENT 'Lignes bronze chargées après cette heure, exclue',
    fin_bronze TIMESTAMP COMMENT 'Dernière heure de chargement bronze traitée, incluse',
    lignes_lues BIGINT COMMENT 'Lignes bronze lues',
    doublons_exacts BIGINT COMMENT 'Copies exactes écartées',
    lignes_silver BIGINT COMMENT 'Lignes valides envoyées au silver',
    lignes_quarantaine BIGINT COMMENT 'Lignes mises en quarantaine'
)
COMMENT 'Une ligne par traitement du silver'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Les lignes nouvelles du bronze
# MAGIC
# MAGIC On repart de la dernière heure de chargement notée dans le journal. La borne haute est figée dès le début : une ligne qui arriverait dans le bronze pendant le traitement sera prise la fois suivante. Les heures sont gardées en texte au format de Spark, pour ne pas dépendre du fuseau horaire de Python.

# COMMAND ----------

DEBUT = spark.table(JOURNAL).agg(F.date_format(F.max("fin_bronze"), FORMAT_HORODATAGE)).first()[0]
bronze = spark.table(BRONZE)
if DEBUT is not None:
    bronze = bronze.where(F.col("_charge_le") > F.to_timestamp(F.lit(DEBUT), FORMAT_HORODATAGE))
FIN = bronze.agg(F.date_format(F.max("_charge_le"), FORMAT_HORODATAGE)).first()[0]

print(f"Dernier chargement bronze déjà traité : {DEBUT or 'aucun, premier traitement'}")
if FIN is None:
    print("Rien de nouveau dans le bronze : le silver est à jour.")
    dbutils.notebook.exit("rien à traiter")

lot = bronze.where(F.col("_charge_le") <= F.to_timestamp(F.lit(FIN), FORMAT_HORODATAGE))
print(f"Lignes à traiter : celles chargées jusqu'à {FIN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Les types
# MAGIC
# MAGIC `try_to_timestamp` et `try_cast` renvoient `NULL` au lieu de lever une erreur quand une valeur ne se lit pas : en mode ANSI, un seul texte inattendu ferait sinon échouer tout le traitement. Les règles de qualité repèrent ensuite ces `NULL`.

# COMMAND ----------

COLONNES_SOURCE = [c for c in spark.table(BRONZE).columns if not c.startswith("_")]

# Les colonnes brutes sont rangées dans une structure `_brut`. Spark ne distingue pas les majuscules des
# minuscules dans les noms de colonnes : sans cela, `TRAIN_NO` (brut) et `train_no` (typé) se confondraient.
brut = lot.select(F.struct(*COLONNES_SOURCE).alias("_brut"), "_rescued_data", "_fichier", "_charge_le")


def b(colonne):
    return F.col(f"_brut.{colonne}")


def rempli(colonne):
    return b(colonne).isNotNull() & (F.trim(b(colonne)) != "")


def texte(colonne):
    """Le texte sans espaces autour, ou NULL s'il est vide. Une colonne absente du CSV donne NULL."""
    if colonne not in COLONNES_SOURCE:
        return F.lit(None).cast("string")
    return F.when(rempli(colonne), F.trim(b(colonne)))


def entier(colonne):
    # Deux try_cast : un texte comme « 120.0 » passe par un décimal.
    return F.expr(f"try_cast(try_cast(trim(_brut.{colonne}) AS DOUBLE) AS INT)")


def horodatage(col_date, col_heure):
    valeur = F.concat_ws(" ", F.trim(b(col_date)), F.trim(b(col_heure)))
    return F.when(rempli(col_date) & rempli(col_heure),
                  F.try_to_timestamp(valeur, F.lit(f"{FORMAT_DATE} {FORMAT_HEURE}")))


def ecart_avec_les_heures(prevu, reel, retard):
    """Écart, en secondes, entre le retard publié et celui qu'on recalcule avec les heures."""
    return F.abs((F.unix_timestamp(reel) - F.unix_timestamp(prevu)) - F.col(retard))


def en_texte(colonne):
    return F.coalesce(F.date_format(colonne, "yyyy-MM-dd HH:mm:ss"), F.lit(""))


typee = (
    brut.select(
        "*",
        F.try_to_timestamp(F.trim(b("DATDEP")), F.lit(FORMAT_DATE)).cast("date").alias("date_depart"),
        entier("TRAIN_NO").alias("train_no"),
        texte("RELATION").alias("relation"),
        texte("RELATION_DIRECTION").alias("sens_relation"),
        texte("TRAIN_SERV").alias("operateur"),
        entier("PTCAR_NO").alias("ptcar_no"),
        texte("PTCAR_LG_NM_NL").alias("point_arret"),
        texte("LINE_NO_DEP").alias("ligne_depart"),
        texte("LINE_NO_ARR").alias("ligne_arrivee"),
        horodatage("PLANNED_DATE_ARR", "PLANNED_TIME_ARR").alias("arrivee_prevue"),
        horodatage("REAL_DATE_ARR", "REAL_TIME_ARR").alias("arrivee_reelle"),
        horodatage("PLANNED_DATE_DEP", "PLANNED_TIME_DEP").alias("depart_prevu"),
        horodatage("REAL_DATE_DEP", "REAL_TIME_DEP").alias("depart_reel"),
        entier("DELAY_ARR").alias("retard_arrivee_s"),
        entier("DELAY_DEP").alias("retard_depart_s"),
        texte("CIRC_TYP").alias("circ_typ"),
        texte("OP1_COD").alias("op1_cod"),
        texte("THOP1_COD").alias("thop1_cod"),
    )
    .withColumn("retard_arrivee_min", F.col("retard_arrivee_s") / 60.0)
    .withColumn("retard_depart_min", F.col("retard_depart_s") / 60.0)
    .withColumn("retard_arrivee_incoherent", F.coalesce(
        ecart_avec_les_heures("arrivee_prevue", "arrivee_reelle", "retard_arrivee_s") > ECART_TOLERE_S, F.lit(False)))
    .withColumn("retard_depart_incoherent", F.coalesce(
        ecart_avec_les_heures("depart_prevu", "depart_reel", "retard_depart_s") > ECART_TOLERE_S, F.lit(False)))
    # La clé du passage, et une empreinte de la ligne brute pour reconnaître une copie exacte.
    .withColumn("id_passage", F.sha2(F.concat_ws(
        "|",
        F.coalesce(F.col("date_depart").cast("string"), F.lit("")),
        F.coalesce(F.col("train_no").cast("string"), F.lit("")),
        F.coalesce(F.col("ptcar_no").cast("string"), F.lit("")),
        en_texte(F.col("arrivee_prevue")),
        en_texte(F.col("depart_prevu")),
    ), 256))
    .withColumn("_id_ligne", F.sha2(F.to_json(F.struct("_brut", "_rescued_data", "_fichier")), 256))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Les règles de qualité
# MAGIC
# MAGIC Chaque règle non respectée ajoute une raison de rejet. Une ligne qui en accumule plusieurs les garde toutes, ce qui aide à comprendre ce qui ne va pas.

# COMMAND ----------

REGLES = {
    "date de départ illisible": F.col("date_depart").isNull(),
    "numéro de train illisible": F.col("train_no").isNull(),
    "point d'arrêt illisible": F.col("ptcar_no").isNull(),
    "ni arrivée ni départ prévus": F.col("arrivee_prevue").isNull() & F.col("depart_prevu").isNull(),
    "heure illisible": (
        (rempli("PLANNED_TIME_ARR") & F.col("arrivee_prevue").isNull())
        | (rempli("REAL_TIME_ARR") & F.col("arrivee_reelle").isNull())
        | (rempli("PLANNED_TIME_DEP") & F.col("depart_prevu").isNull())
        | (rempli("REAL_TIME_DEP") & F.col("depart_reel").isNull())
    ),
    "retard illisible": (
        (rempli("DELAY_ARR") & F.col("retard_arrivee_s").isNull())
        | (rempli("DELAY_DEP") & F.col("retard_depart_s").isNull())
    ),
    "avance de plus de 30 min": (F.col("retard_arrivee_s") < -AVANCE_MAX_S) | (F.col("retard_depart_s") < -AVANCE_MAX_S),
    "valeurs hors schéma": F.col("_rescued_data").isNotNull(),
}
raisons = F.filter(
    F.array(*[F.when(condition, F.lit(nom)) for nom, condition in REGLES.items()]),
    lambda raison: raison.isNotNull(),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Les doublons
# MAGIC
# MAGIC Deux cas bien différents :
# MAGIC
# MAGIC - **une copie exacte** d'une ligne n'apporte rien : on l'écarte et on la compte ;
# MAGIC - **une même clé avec un contenu différent** demande un choix. On garde la ligne qui a le plus d'heures et de retards réels renseignés, puis celle qui a un vrai retard mesuré, puis la plus récemment chargée ; les autres partent en quarantaine avec la raison « doublon de clé ». Le dernier critère, l'empreinte de la ligne, ne sert qu'à rendre le choix reproductible.
# MAGIC
# MAGIC Le deuxième critère vient des vraies données : l'EXTRA n° 18201 du 26 septembre 2026 existe en deux versions aussi complètes l'une que l'autre. L'une recopie l'horaire, avec un retard nul à la seconde près à chaque point ; l'autre a un retard qui grandit normalement le long du trajet. Sans ce critère, le choix se faisait point par point sur l'empreinte, et le silver mélangeait les deux versions.
# MAGIC
# MAGIC Le lot ainsi classé est écrit une fois dans une table de travail, `lot_en_cours` : les étapes suivantes le relisent sans tout recalculer, et il reste disponible pour comprendre si le notebook s'arrête en route.

# COMMAND ----------

copies = Window.partitionBy("_id_ligne").orderBy(F.col("_charge_le").desc())
completude = sum(F.col(c).isNotNull().cast("int") for c in ["arrivee_reelle", "depart_reel", "retard_arrivee_s", "retard_depart_s"])
# Un retard nul à la seconde près, à l'arrivée comme au départ : la ligne recopie l'horaire au lieu de le mesurer.
recopie_horaire = (F.coalesce(F.col("retard_arrivee_s"), F.lit(0)) == 0) & (F.coalesce(F.col("retard_depart_s"), F.lit(0)) == 0)
par_cle = Window.partitionBy("id_passage").orderBy(
    (F.col("_copie") > 1).cast("int"),   # les copies exactes passent en dernier
    F.size("raisons"),                    # puis les lignes déjà rejetées pour une autre raison
    F.col("_completude").desc(),          # puis les lignes les moins complètes
    F.col("_recopie_horaire").cast("int"),  # puis celles qui recopient l'horaire
    F.col("_charge_le").desc(),
    F.col("_id_ligne"),
)

(
    typee
    .withColumn("raisons", raisons)
    .withColumn("_copie", F.row_number().over(copies))
    .withColumn("_completude", completude)
    .withColumn("_recopie_horaire", recopie_horaire)
    .withColumn("_rang", F.row_number().over(par_cle))
    .withColumn("raisons", F.when(
        (F.col("_copie") == 1) & (F.col("_rang") > 1) & (F.size("raisons") == 0), F.array(F.lit("doublon de clé"))
    ).otherwise(F.col("raisons")))
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(LOT)
)

classee = spark.table(LOT)
valides = classee.where((F.col("_copie") == 1) & (F.size("raisons") == 0))
rejetees = classee.where((F.col("_copie") == 1) & (F.size("raisons") > 0))

bilan = classee.agg(
    F.count(F.lit(1)).alias("lues"),
    F.sum((F.col("_copie") > 1).cast("int")).alias("doublons_exacts"),
    F.sum(((F.col("_copie") == 1) & (F.size("raisons") == 0)).cast("int")).alias("silver"),
    F.sum(((F.col("_copie") == 1) & (F.size("raisons") > 0)).cast("int")).alias("quarantaine"),
    F.sum(((F.col("_copie") == 1) & (F.size("raisons") == 0) & F.col("retard_arrivee_incoherent")).cast("int")).alias("arrivees_incoherentes"),
    F.sum(((F.col("_copie") == 1) & (F.size("raisons") == 0) & F.col("retard_depart_incoherent")).cast("int")).alias("departs_incoherents"),
).first()
LUES, DOUBLONS_EXACTS = int(bilan["lues"]), int(bilan["doublons_exacts"] or 0)
LIGNES_SILVER, LIGNES_QUARANTAINE = int(bilan["silver"] or 0), int(bilan["quarantaine"] or 0)

print(f"Lignes lues : {nombre(LUES)}")
print(f"Copies exactes écartées : {nombre(DOUBLONS_EXACTS)}")
print(f"Lignes valides pour le silver : {nombre(LIGNES_SILVER)}")
print(f"  dont arrivées avec un retard incohérent : {nombre(bilan['arrivees_incoherentes'])}")
print(f"  dont départs avec un retard incohérent : {nombre(bilan['departs_incoherents'])}")
print(f"Lignes mises en quarantaine : {nombre(LIGNES_QUARANTAINE)}")

# COMMAND ----------

# Les raisons de rejet de ce lot.
display(rejetees.select(F.explode("raisons").alias("raison")).groupBy("raison").count().orderBy(F.desc("count")))

# COMMAND ----------

# Les clés en double, ligne à ligne : le rang 1 va au silver, les autres en quarantaine.
cles_doublees = classee.where("_copie = 1").groupBy("id_passage").count().where("count > 1").select("id_passage")
display(
    classee.join(cles_doublees, "id_passage")
    .select("_rang", "date_depart", "train_no", "relation", "ptcar_no", "point_arret",
            "arrivee_prevue", "arrivee_reelle", "retard_arrivee_s", "depart_prevu", "depart_reel", "retard_depart_s",
            "op1_cod", "thop1_cod", "raisons", "_fichier")
    .orderBy("date_depart", "train_no", "arrivee_prevue", "depart_prevu", "_rang")
    .limit(40)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. L'écriture avec MERGE
# MAGIC
# MAGIC Le `MERGE` du silver ajoute les passages nouveaux, et ne remplace un passage déjà connu que si la ligne vient d'un chargement plus récent. Celui de la quarantaine n'ajoute que les lignes qu'elle ne contient pas encore. Rejouer un lot ne change donc rien : c'est ce qui rend le notebook sûr à relancer.

# COMMAND ----------

COLONNES_SILVER = spark.table(SILVER).columns
valides.withColumn("_traite_le", F.current_timestamp()).select(*COLONNES_SILVER).createOrReplaceTempView("lot_silver")
display(spark.sql(f"""
    MERGE INTO {SILVER} AS cible
    USING lot_silver AS source
    ON cible.id_passage = source.id_passage
    WHEN MATCHED AND source._charge_le > cible._charge_le THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
"""))

# COMMAND ----------

COLONNES_QUARANTAINE = spark.table(QUARANTAINE).columns
(
    rejetees
    .withColumn("ligne_brute", F.to_json("_brut"))
    .withColumn("_mis_en_quarantaine_le", F.current_timestamp())
    .select(*COLONNES_QUARANTAINE)
    .createOrReplaceTempView("lot_quarantaine")
)
display(spark.sql(f"""
    MERGE INTO {QUARANTAINE} AS cible
    USING lot_quarantaine AS source
    ON cible._id_ligne = source._id_ligne
    WHEN NOT MATCHED THEN INSERT *
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Les contrôles après écriture
# MAGIC
# MAGIC Trois vérifications, qui arrêtent le notebook, et donc le job, si l'une échoue :
# MAGIC
# MAGIC 1. chaque passage valide du lot se retrouve dans le silver ;
# MAGIC 2. chaque ligne rejetée se retrouve dans la quarantaine ;
# MAGIC 3. le silver n'a aucune clé en double.

# COMMAND ----------

absents_silver = valides.select("id_passage").join(spark.table(SILVER).select("id_passage"), "id_passage", "left_anti").count()
absents_quarantaine = rejetees.select("_id_ligne").join(spark.table(QUARANTAINE).select("_id_ligne"), "_id_ligne", "left_anti").count()
cles_en_double = spark.table(SILVER).groupBy("id_passage").count().where("count > 1").count()

print(f"Passages valides absents du silver : {nombre(absents_silver)}")
print(f"Lignes rejetées absentes de la quarantaine : {nombre(absents_quarantaine)}")
print(f"Clés en double dans le silver : {nombre(cles_en_double)}")
erreurs = [
    message for message, probleme in [
        (f"{absents_silver} passage(s) valide(s) absent(s) du silver", absents_silver),
        (f"{absents_quarantaine} ligne(s) rejetée(s) absente(s) de la quarantaine", absents_quarantaine),
        (f"{cles_en_double} clé(s) en double dans le silver", cles_en_double),
    ] if probleme
]
if erreurs:
    raise AssertionError(" ; ".join(erreurs))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Le journal
# MAGIC
# MAGIC Le traitement n'est noté qu'à la fin, une fois tout écrit et contrôlé. Si le notebook s'arrête avant, le lot sera simplement retraité la fois suivante.

# COMMAND ----------

debut_sql = f"to_timestamp('{DEBUT}', '{FORMAT_HORODATAGE}')" if DEBUT else "CAST(NULL AS TIMESTAMP)"
spark.sql(f"""
    INSERT INTO {JOURNAL}
    SELECT current_timestamp(), {debut_sql}, to_timestamp('{FIN}', '{FORMAT_HORODATAGE}'),
           {LUES}, {DOUBLONS_EXACTS}, {LIGNES_SILVER}, {LIGNES_QUARANTAINE}
""")
spark.sql(f"DROP TABLE IF EXISTS {LOT}")
display(spark.table(JOURNAL).orderBy(F.desc("traite_le")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Le silver en chiffres

# COMMAND ----------

silver = spark.table(SILVER)
display(silver.agg(
    F.count(F.lit(1)).alias("passages"),
    F.countDistinct("date_depart", "train_no").alias("trains_jours"),
    F.countDistinct("ptcar_no").alias("points"),
    F.min("date_depart").alias("premier_jour"),
    F.max("date_depart").alias("dernier_jour"),
    F.sum(F.col("retard_arrivee_incoherent").cast("int")).alias("arrivees_incoherentes"),
))
display(silver.orderBy("date_depart", "train_no", "arrivee_prevue").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## La suite
# MAGIC
# MAGIC Le notebook 04, en SQL, construit le gold à partir de `silver.passages` : la table de faits des passages et ses dimensions, puis les vues pour le tableau de bord.
