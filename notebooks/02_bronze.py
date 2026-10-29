# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze : les CSV dans une table Delta
# MAGIC
# MAGIC Ce notebook charge les fichiers mensuels d'Infrabel dans la table Delta `bronze.passages_bruts`, sans rien transformer : les colonnes restent du texte, exactement comme dans les CSV. Il ajoute seulement des colonnes techniques : le fichier source, sa date de modification et l'heure du chargement.
# MAGIC
# MAGIC Le chargement passe par **Auto Loader**, avec le déclencheur `availableNow` : à chaque exécution, il lit les fichiers qu'il n'a pas encore vus, puis s'arrête. Un mois déjà chargé ne l'est jamais une deuxième fois, et le notebook peut tourner chaque mois dans le job sans rien changer.
# MAGIC
# MAGIC Ce que le notebook 01 a montré, et que ce notebook applique :
# MAGIC
# MAGIC - le séparateur se lit sur l'en-tête, et l'encodage est l'UTF-8 ;
# MAGIC - les fichiers ont 22 colonnes : les 19 de la documentation, plus `CIRC_TYP`, `OP1_COD` et `THOP1_COD`, gardées telles quelles ;
# MAGIC - les conversions de type (des dates comme `01AUG2026`, des heures comme `9:59:59`) se font en silver, pas ici.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paramètres
# MAGIC
# MAGIC Les widgets reprennent ceux du notebook 01. Le widget `reinitialiser` sert pendant le développement : sur « oui », il supprime la table et la mémoire d'Auto Loader, et tous les fichiers du volume sont rechargés. Il faut ensuite le remettre sur « non ».

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalogue", "workspace")
dbutils.widgets.text("schema", "bronze")
dbutils.widgets.text("volume", "fichiers")
dbutils.widgets.text("encodage", "UTF-8")
dbutils.widgets.dropdown("reinitialiser", "non", ["non", "oui"])

CATALOGUE = dbutils.widgets.get("catalogue")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")
ENCODAGE = dbutils.widgets.get("encodage")
REINITIALISER = dbutils.widgets.get("reinitialiser") == "oui"

SOURCE = f"/Volumes/{CATALOGUE}/{SCHEMA}/{VOLUME}"
TABLE = f"{CATALOGUE}.{SCHEMA}.passages_bruts"

# Auto Loader garde, dans un volume à part, le schéma qu'il a déduit des fichiers
# et la liste des fichiers déjà lus (le checkpoint). C'est ce qui évite de charger deux fois un mois.
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGUE}.{SCHEMA}.checkpoints COMMENT 'Schémas et checkpoints Auto Loader'")
TECHNIQUE = f"/Volumes/{CATALOGUE}/{SCHEMA}/checkpoints/passages_bruts"


def nombre(x, decimales=0):
    """Un nombre écrit à la française : espace pour les milliers, virgule pour les décimales."""
    return f"{x:,.{decimales}f}".replace(",", " ").replace(".", ",")


print(f"Fichiers lus dans : {SOURCE}")
print(f"Table écrite : {TABLE}")

# COMMAND ----------

if REINITIALISER:
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
    dbutils.fs.rm(TECHNIQUE, True)
    print("Table et checkpoint supprimés : tous les fichiers du volume vont être rechargés.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Les fichiers à charger
# MAGIC
# MAGIC Le séparateur se lit sur la première ligne du premier fichier, comme dans le notebook 01. On note aussi l'état de la table avant le chargement, pour savoir ensuite ce qui est nouveau.

# COMMAND ----------

fichiers = sorted(
    [f for f in dbutils.fs.ls(SOURCE) if f.name.lower().endswith(".csv")],
    key=lambda f: f.name,
)
if not fichiers:
    raise ValueError(f"Aucun fichier CSV dans {SOURCE}. Dépose d'abord les fichiers mensuels dans le volume.")

premiere_ligne = spark.read.text(fichiers[0].path).first()[0]
SEPARATEUR = ";" if premiere_ligne.count(";") > premiere_ligne.count(",") else ","

# L'heure du dernier chargement est gardée en texte, au format de Spark, pour la comparer ensuite
# sans dépendre du fuseau horaire de Python.
FORMAT_HEURE = "yyyy-MM-dd HH:mm:ss.SSSSSS"
TABLE_EXISTAIT = spark.catalog.tableExists(TABLE)
DERNIER_CHARGEMENT = (
    spark.table(TABLE).agg(F.date_format(F.max("_charge_le"), FORMAT_HEURE)).first()[0]
    if TABLE_EXISTAIT else None
)

print(f"{len(fichiers)} fichier(s) CSV dans le volume, séparateur '{SEPARATEUR}'")
print(f"Dernier chargement dans la table : {DERNIER_CHARGEMENT or 'aucun, la table va être créée'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Le chargement avec Auto Loader
# MAGIC
# MAGIC `cloudFiles.inferColumnTypes` à `false` garde toutes les colonnes en texte : en bronze, on ne décide encore rien. Auto Loader ajoute de lui-même la colonne `_rescued_data`, qui recueille ce qui ne rentre pas dans le schéma au lieu de le perdre.
# MAGIC
# MAGIC Si Infrabel ajoute un jour une colonne, Auto Loader s'arrête une fois avec une erreur `UnknownFieldException`, après avoir noté la nouvelle colonne. Il suffit de relancer le notebook : grâce à `mergeSchema`, la colonne est ajoutée à la table.

# COMMAND ----------

flux = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{TECHNIQUE}/schema")
    .option("cloudFiles.inferColumnTypes", "false")
    .option("header", "true")
    .option("sep", SEPARATEUR)
    .option("encoding", ENCODAGE)
    .option("pathGlobFilter", "*.[cC][sS][vV]")
    .load(SOURCE)
    .select(
        "*",
        F.col("_metadata.file_name").alias("_fichier"),
        F.col("_metadata.file_modification_time").alias("_fichier_modifie_le"),
        F.current_timestamp().alias("_charge_le"),
    )
)

requete = (
    flux.writeStream
    .option("checkpointLocation", f"{TECHNIQUE}/checkpoint")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TABLE)
)
requete.awaitTermination()

# Sur Databricks, le premier rapport d'avancement peut arriver sans nombre de lignes : d'où le « or 0 ».
LIGNES_LUES = sum(p["numInputRows"] or 0 for p in requete.recentProgress)
print(f"Lignes chargées pendant cette exécution : {nombre(LIGNES_LUES)}")

# COMMAND ----------

# Les commentaires apparaissent dans l'explorateur de catalogue. On les pose une seule fois,
# à la création, pour ne pas ajouter une version à la table à chaque exécution.
if not TABLE_EXISTAIT:
    spark.sql(f"COMMENT ON TABLE {TABLE} IS 'Passages bruts Infrabel : CSV mensuels chargés tels quels par Auto Loader, toutes les colonnes en texte'")
    for colonne, commentaire in {
        "_fichier": "Nom du CSV source",
        "_fichier_modifie_le": "Date de modification du CSV dans le volume",
        "_charge_le": "Heure du chargement par Auto Loader",
        "_rescued_data": "Valeurs qui ne rentraient pas dans le schéma, en JSON",
    }.items():
        spark.sql(f"ALTER TABLE {TABLE} ALTER COLUMN {colonne} COMMENT '{commentaire}'")

display(
    spark.table(TABLE)
    .groupBy("_fichier")
    .agg(F.count(F.lit(1)).alias("lignes"), F.max("_charge_le").alias("charge_le"))
    .orderBy("_fichier")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Le contrôle : rien de perdu, rien en double
# MAGIC
# MAGIC Pour chaque fichier chargé pendant cette exécution, on recompte les lignes du CSV et on les compare à celles de la table. Les deux nombres doivent être égaux. Si un fichier manque, ou s'il a été chargé deux fois, le notebook s'arrête ici avec une erreur : dans le job, le silver et le gold ne tournent alors pas sur des données fausses.

# COMMAND ----------

nouveaux = spark.table(TABLE)
if DERNIER_CHARGEMENT is not None:
    nouveaux = nouveaux.where(F.col("_charge_le") > F.to_timestamp(F.lit(DERNIER_CHARGEMENT), FORMAT_HEURE))
noms = sorted(r["_fichier"] for r in nouveaux.select("_fichier").distinct().collect())

if not noms:
    print("Aucun nouveau fichier : la table était déjà à jour.")
else:
    lignes_csv = (
        spark.read.option("header", True).option("sep", SEPARATEUR).option("encoding", ENCODAGE)
        .csv([f"{SOURCE}/{nom}" for nom in noms])
        .groupBy(F.col("_metadata.file_name").alias("_fichier"))
        .agg(F.count(F.lit(1)).alias("lignes_csv"))
    )
    lignes_table = (
        spark.table(TABLE).where(F.col("_fichier").isin(noms))
        .groupBy("_fichier").agg(F.count(F.lit(1)).alias("lignes_table"))
    )
    controle = (
        lignes_csv.join(lignes_table, "_fichier", "full_outer")
        .withColumn("ecart", F.coalesce("lignes_table", F.lit(0)) - F.coalesce("lignes_csv", F.lit(0)))
        .orderBy("_fichier")
    )
    display(controle)
    en_erreur = [r["_fichier"] for r in controle.where("ecart != 0").collect()]
    if en_erreur:
        raise AssertionError(f"Le nombre de lignes diffère entre le CSV et la table pour : {', '.join(en_erreur)}")
    print(f"{len(noms)} fichier(s) contrôlé(s) : la table a exactement les lignes des CSV.")

# COMMAND ----------

# Les lignes qu'Auto Loader n'a pas pu ranger dans le schéma. Elles ne bloquent rien ici : le silver les mettra en quarantaine.
hors_schema = spark.table(TABLE).where(F.col("_rescued_data").isNotNull())
NB_HORS_SCHEMA = hors_schema.count()
print(f"Lignes avec des valeurs hors schéma (_rescued_data) : {nombre(NB_HORS_SCHEMA)}")
if NB_HORS_SCHEMA:
    display(hors_schema.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ce que Delta apporte
# MAGIC
# MAGIC Une table Delta, ce sont des fichiers Parquet, compressés et rangés par colonne, plus un journal des modifications. D'abord le poids : les CSV du volume face à la table.

# COMMAND ----------

TAILLE_CSV = sum(f.size for f in fichiers)
detail = spark.sql(f"DESCRIBE DETAIL {TABLE}").first()
print(f"CSV dans le volume : {nombre(TAILLE_CSV / 1024 ** 2, 1)} Mo pour {len(fichiers)} fichier(s)")
print(f"Table Delta : {nombre(detail['sizeInBytes'] / 1024 ** 2, 1)} Mo en {detail['numFiles']} fichier(s) Parquet")
print(f"Soit {nombre(TAILLE_CSV / detail['sizeInBytes'], 1)} fois moins de place")

# COMMAND ----------

# MAGIC %md
# MAGIC Ensuite le journal : chaque chargement crée une version de la table. On peut relire une version passée avec `VERSION AS OF`, ou revenir en arrière avec `RESTORE` si un chargement s'est mal passé.

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {TABLE}")
    .select("version", "timestamp", "operation", F.col("operationMetrics")["numOutputRows"].alias("lignes_ecrites"))
    .orderBy(F.desc("version"))
)

# COMMAND ----------

display(spark.table(TABLE).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## La suite
# MAGIC
# MAGIC Le notebook 03 lit cette table et construit le silver : les types, la clé, le dédoublonnage et les règles de qualité, avec une quarantaine pour les lignes rejetées.
