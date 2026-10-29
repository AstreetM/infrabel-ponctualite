-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 · Gold : le schéma en étoile, en SQL
-- MAGIC
-- MAGIC Ce notebook construit le gold à partir de `silver.passages` :
-- MAGIC
-- MAGIC - une table de faits, `fait_passage`, au grain du passage : un train, à un point, un jour ;
-- MAGIC - cinq dimensions : `dim_date`, `dim_heure`, `dim_gare`, `dim_relation` et `dim_operateur` ;
-- MAGIC - des vues d'agrégats, prêtes pour le tableau de bord ;
-- MAGIC - des contrôles qui arrêtent le notebook si le modèle n'est pas cohérent.
-- MAGIC
-- MAGIC Tout est reconstruit à chaque exécution avec `CREATE OR REPLACE` : le gold n'est fait que de calculs sur le silver, il n'a rien à garder d'une fois sur l'autre. Delta conserve quand même chaque version dans l'historique des tables.
-- MAGIC
-- MAGIC Si le catalogue ne s'appelle pas `workspace`, il suffit de changer son nom dans la cellule `USE CATALOG` juste en dessous.

-- COMMAND ----------

USE CATALOG workspace

-- COMMAND ----------

-- Les heures du silver sont des heures belges lues sans conversion : on garde le même fuseau pour en extraire l'heure.
SET TIME ZONE 'Etc/UTC'

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS gold COMMENT 'Schéma en étoile et vues pour le tableau de bord'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Les dimensions
-- MAGIC
-- MAGIC ### `dim_date`
-- MAGIC
-- MAGIC Un jour par ligne, du premier au dernier jour du silver, avec les jours fériés belges. La clé `date_cle` est un entier au format `aaaammjj`, comme le veut l'usage dans un entrepôt. La liste des jours fériés couvre 2025 et 2026 ; elle est à compléter pour d'autres années.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_date
COMMENT 'Un jour par ligne sur la période du silver, avec les jours fériés belges'
AS
WITH bornes AS (
    SELECT min(date_depart) AS debut, max(date_depart) AS fin
    FROM silver.passages
),
jours AS (
    SELECT explode(sequence(debut, fin, INTERVAL 1 DAY)) AS date_jour
    FROM bornes
),
feries AS (
    SELECT * FROM VALUES
        (DATE'2025-01-01', 'Nouvel An'),
        (DATE'2025-04-21', 'Lundi de Pâques'),
        (DATE'2025-05-01', 'Fête du Travail'),
        (DATE'2025-05-29', 'Ascension'),
        (DATE'2025-06-09', 'Lundi de Pentecôte'),
        (DATE'2025-07-21', 'Fête nationale'),
        (DATE'2025-08-15', 'Assomption'),
        (DATE'2025-11-01', 'Toussaint'),
        (DATE'2025-11-11', 'Armistice'),
        (DATE'2025-12-25', 'Noël'),
        (DATE'2026-01-01', 'Nouvel An'),
        (DATE'2026-04-06', 'Lundi de Pâques'),
        (DATE'2026-05-01', 'Fête du Travail'),
        (DATE'2026-05-14', 'Ascension'),
        (DATE'2026-05-25', 'Lundi de Pentecôte'),
        (DATE'2026-07-21', 'Fête nationale'),
        (DATE'2026-08-15', 'Assomption'),
        (DATE'2026-11-01', 'Toussaint'),
        (DATE'2026-11-11', 'Armistice'),
        (DATE'2026-12-25', 'Noël')
        AS feries(date_ferie, nom_ferie)
)
SELECT
    CAST(date_format(j.date_jour, 'yyyyMMdd') AS INT) AS date_cle,
    j.date_jour,
    year(j.date_jour) AS annee,
    month(j.date_jour) AS mois,
    element_at(array('janvier', 'février', 'mars', 'avril', 'mai', 'juin', 'juillet', 'août',
                     'septembre', 'octobre', 'novembre', 'décembre'), month(j.date_jour)) AS nom_mois,
    date_format(j.date_jour, 'yyyy-MM') AS annee_mois,
    day(j.date_jour) AS jour,
    weekday(j.date_jour) + 1 AS jour_semaine,
    element_at(array('lundi', 'mardi', 'mercredi', 'jeudi', 'vendredi', 'samedi', 'dimanche'),
               weekday(j.date_jour) + 1) AS nom_jour,
    weekday(j.date_jour) >= 5 AS est_weekend,
    f.nom_ferie IS NOT NULL AS est_ferie,
    f.nom_ferie,
    CASE WHEN weekday(j.date_jour) >= 5 OR f.nom_ferie IS NOT NULL THEN 'week-end ou férié'
         ELSE 'jour ouvrable' END AS type_jour
FROM jours j
LEFT JOIN feries f ON j.date_jour = f.date_ferie

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### `dim_heure`
-- MAGIC
-- MAGIC Une minute de la journée par ligne, soit 1 440 lignes. La clé `minute_cle` compte les minutes depuis minuit. Les pointes suivent ce que l'exploration a montré : le retard monte de 7 h à 9 h, puis en fin d'après-midi.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_heure
COMMENT 'Une minute de la journée par ligne, avec sa tranche horaire'
AS
SELECT
    m AS minute_cle,
    m DIV 60 AS heure,
    m % 60 AS minute,
    format_string('%02d:%02d', m DIV 60, m % 60) AS heure_minute,
    CASE
        WHEN m DIV 60 IN (7, 8) THEN 'pointe du matin'
        WHEN m DIV 60 BETWEEN 9 AND 15 THEN 'journée'
        WHEN m DIV 60 IN (16, 17, 18) THEN 'pointe du soir'
        WHEN m DIV 60 BETWEEN 19 AND 22 THEN 'soirée'
        ELSE 'nuit et petit matin'
    END AS tranche,
    CASE
        WHEN m DIV 60 IN (7, 8) THEN 2
        WHEN m DIV 60 BETWEEN 9 AND 15 THEN 3
        WHEN m DIV 60 IN (16, 17, 18) THEN 4
        WHEN m DIV 60 BETWEEN 19 AND 22 THEN 5
        ELSE 1
    END AS ordre_tranche,
    m DIV 60 IN (7, 8, 16, 17, 18) AS est_pointe
FROM (SELECT explode(sequence(0, 1439)) AS m)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### `dim_gare`, `dim_relation` et `dim_operateur`
-- MAGIC
-- MAGIC - **`dim_gare`** : un point du réseau par ligne. Ce n'est pas toujours une gare : certains points sont de simples points de passage. L'exploration a montré que chaque numéro garde un seul nom. Les cinq points de la jonction Nord-Midi, à Bruxelles, sont marqués pour la question 5 du projet. Les noms viennent de la source, en néerlandais.
-- MAGIC - **`dim_relation`** : une relation et son sens par ligne, avec le type de train déduit du code (IC, L, S, P…). Sur août et septembre 2026, un seul code manquait au départ : EURST, l'Eurostar (ex-Thalys), rangé avec les trains internationaux. Un code inconnu tombe dans « Autre » ; la section 5 liste ceux qui restent.
-- MAGIC - **`dim_operateur`** : un opérateur par ligne, avec un nom lisible.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_gare
COMMENT 'Un point du réseau par ligne : gare, halte ou point de passage'
AS
SELECT
    ptcar_no,
    max_by(point_arret, _charge_le) AS nom_nl,
    max_by(point_arret, _charge_le) RLIKE '^BRUSSEL-(NOORD|CONGRES|CENTRAAL|KAPELLEKERK|ZUID)$' AS est_jonction_nord_midi
FROM silver.passages
GROUP BY ptcar_no

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_relation
COMMENT 'Une relation commerciale et son sens par ligne, avec le type de train'
AS
SELECT
    sha2(concat_ws('|', coalesce(relation, ''), coalesce(sens_relation, '')), 256) AS relation_cle,
    relation,
    sens_relation,
    code_type,
    CASE
        WHEN code_type = 'IC' THEN 'InterCity'
        WHEN code_type = 'L' THEN 'Omnibus'
        WHEN code_type = 'S' THEN 'Train S'
        WHEN code_type = 'P' THEN 'Train de pointe'
        WHEN code_type = 'EXTRA' THEN 'Train supplémentaire'
        WHEN code_type = 'CHARTER' THEN 'Train affrété'
        -- EURST : Eurostar, ex-Thalys (opérateur THI-FACT)
        WHEN code_type IN ('EURST', 'ICE', 'INT', 'EC', 'THAL', 'EUR', 'TGV') THEN 'International'
        ELSE 'Autre'
    END AS type_train
FROM (
    SELECT DISTINCT relation, sens_relation, regexp_extract(relation, '^([A-Z]+)', 1) AS code_type
    FROM silver.passages
)

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_operateur
COMMENT 'Un opérateur ferroviaire par ligne'
AS
SELECT DISTINCT
    operateur AS operateur_code,
    CASE operateur
        WHEN 'SNCB/NMBS' THEN 'SNCB'
        WHEN 'EURSLEEPER' THEN 'European Sleeper'
        WHEN 'THI-FACT' THEN 'Eurostar (ex-Thalys)'
        ELSE operateur
    END AS operateur_nom
FROM silver.passages

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. La table de faits
-- MAGIC
-- MAGIC Une ligne par passage du silver. Les points d'un même train, un même jour, sont classés par heure prévue : c'est ce qui donne le rang de l'arrêt, le premier et le dernier point du trajet, et le retard pris en route.
-- MAGIC
-- MAGIC - **`a_l_heure`** : vrai si le train arrive avec moins de 6 minutes de retard, la règle d'Infrabel. Vide s'il n'y a pas d'arrivée mesurée.
-- MAGIC - **`est_terminus`** : le dernier point du trajet. C'est là qu'Infrabel mesure la ponctualité. Pour un train qui quitte la Belgique, c'est le dernier point connu.
-- MAGIC - **`retard_pris_en_ligne_min`** : le retard gagné entre le départ du point précédent et l'arrivée à celui-ci.
-- MAGIC - **`retard_pris_a_quai_min`** : le retard gagné pendant l'arrêt, entre l'arrivée et le départ.
-- MAGIC - **`train_no`** : une dimension dégénérée, comme un numéro de commande dans une table de ventes.

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.fait_passage
COMMENT 'Un passage par ligne : un train, à un point, un jour, avec ses retards et sa place dans le trajet'
AS
WITH ordonnes AS (
    SELECT
        p.*,
        coalesce(p.arrivee_prevue, p.depart_prevu) AS heure_prevue,
        row_number() OVER trajet AS rang_arret,
        count(*) OVER (PARTITION BY p.date_depart, p.train_no) AS nb_arrets,
        lag(p.retard_depart_min) OVER trajet AS retard_depart_precedent_min
    FROM silver.passages p
    WINDOW trajet AS (
        PARTITION BY p.date_depart, p.train_no
        ORDER BY coalesce(p.arrivee_prevue, p.depart_prevu), p.id_passage
    )
)
SELECT
    id_passage,
    CAST(date_format(date_depart, 'yyyyMMdd') AS INT) AS date_cle,
    hour(heure_prevue) * 60 + minute(heure_prevue) AS minute_cle,
    ptcar_no,
    sha2(concat_ws('|', coalesce(relation, ''), coalesce(sens_relation, '')), 256) AS relation_cle,
    operateur AS operateur_code,
    train_no,
    date_depart,
    arrivee_prevue,
    arrivee_reelle,
    depart_prevu,
    depart_reel,
    retard_arrivee_min,
    retard_depart_min,
    CASE WHEN retard_arrivee_min IS NOT NULL THEN retard_arrivee_min < 6 END AS a_l_heure,
    rang_arret,
    nb_arrets,
    rang_arret = 1 AS est_origine,
    rang_arret = nb_arrets AS est_terminus,
    retard_arrivee_min - retard_depart_precedent_min AS retard_pris_en_ligne_min,
    retard_depart_min - retard_arrivee_min AS retard_pris_a_quai_min,
    retard_arrivee_incoherent OR retard_depart_incoherent AS retard_incoherent
FROM ordonnes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Les contrôles
-- MAGIC
-- MAGIC Chaque contrôle compte des écarts. S'il en trouve un seul, la cellule s'arrête avec une erreur, et le job aussi.

-- COMMAND ----------

WITH controles AS (
    SELECT 'le fait a autant de lignes que le silver' AS controle,
           (SELECT count(*) FROM gold.fait_passage) - (SELECT count(*) FROM silver.passages) AS ecarts
    UNION ALL
    SELECT 'chaque passage a un jour dans dim_date',
           (SELECT count(*) FROM gold.fait_passage f LEFT ANTI JOIN gold.dim_date d ON f.date_cle = d.date_cle)
    UNION ALL
    SELECT 'chaque passage a une minute dans dim_heure',
           (SELECT count(*) FROM gold.fait_passage f LEFT ANTI JOIN gold.dim_heure h ON f.minute_cle = h.minute_cle)
    UNION ALL
    SELECT 'chaque passage a un point dans dim_gare',
           (SELECT count(*) FROM gold.fait_passage f LEFT ANTI JOIN gold.dim_gare g ON f.ptcar_no = g.ptcar_no)
    UNION ALL
    SELECT 'chaque passage a une relation dans dim_relation',
           (SELECT count(*) FROM gold.fait_passage f LEFT ANTI JOIN gold.dim_relation r ON f.relation_cle = r.relation_cle)
    UNION ALL
    SELECT 'chaque passage a un opérateur dans dim_operateur',
           (SELECT count(*) FROM gold.fait_passage f LEFT ANTI JOIN gold.dim_operateur o ON f.operateur_code = o.operateur_code)
    UNION ALL
    SELECT 'chaque train a un seul terminus par jour',
           (SELECT count(*) FROM (SELECT date_depart, train_no FROM gold.fait_passage
                                  GROUP BY date_depart, train_no HAVING count_if(est_terminus) <> 1))
)
SELECT
    controle,
    ecarts,
    CASE WHEN ecarts <> 0 THEN raise_error(concat('Contrôle en échec : ', controle)) END AS erreur
FROM controles

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Les vues pour le tableau de bord
-- MAGIC
-- MAGIC Une vue par question du projet. Les vues font les jointures avec les dimensions : le tableau de bord n'a plus qu'à choisir ses colonnes.

-- COMMAND ----------

-- Question 1 : la ponctualité au terminus, mois par mois, comparable aux chiffres officiels d'Infrabel.
CREATE OR REPLACE VIEW gold.v_ponctualite_mois
COMMENT 'Ponctualité au terminus par mois'
AS
SELECT
    d.annee_mois,
    d.annee,
    d.mois,
    d.nom_mois,
    count(*) AS trains,
    round(100 * avg(CAST(f.a_l_heure AS DOUBLE)), 2) AS pct_a_l_heure,
    round(avg(f.retard_arrivee_min), 2) AS retard_moyen_min
FROM gold.fait_passage f
JOIN gold.dim_date d ON f.date_cle = d.date_cle
WHERE f.est_terminus AND f.a_l_heure IS NOT NULL
GROUP BY d.annee_mois, d.annee, d.mois, d.nom_mois

-- COMMAND ----------

-- Question 1, jour par jour.
CREATE OR REPLACE VIEW gold.v_ponctualite_jour
COMMENT 'Ponctualité au terminus par jour'
AS
SELECT
    d.date_jour,
    d.nom_jour,
    d.type_jour,
    d.nom_ferie,
    count(*) AS trains,
    round(100 * avg(CAST(f.a_l_heure AS DOUBLE)), 2) AS pct_a_l_heure,
    round(avg(f.retard_arrivee_min), 2) AS retard_moyen_min
FROM gold.fait_passage f
JOIN gold.dim_date d ON f.date_cle = d.date_cle
WHERE f.est_terminus AND f.a_l_heure IS NOT NULL
GROUP BY d.date_jour, d.nom_jour, d.type_jour, d.nom_ferie

-- COMMAND ----------

-- Question 2 : les points du réseau. La médiane résiste mieux que la moyenne aux retards de plusieurs heures.
CREATE OR REPLACE VIEW gold.v_ponctualite_gare
COMMENT 'Ponctualité en arrivant à chaque point du réseau'
AS
SELECT
    g.ptcar_no,
    g.nom_nl,
    g.est_jonction_nord_midi,
    count(*) AS arrivees,
    round(100 * avg(CAST(f.a_l_heure AS DOUBLE)), 2) AS pct_a_l_heure,
    round(avg(f.retard_arrivee_min), 2) AS retard_moyen_min,
    round(percentile_approx(f.retard_arrivee_min, 0.5), 2) AS retard_median_min,
    round(avg(f.retard_pris_en_ligne_min), 2) AS retard_pris_en_ligne_moyen_min
FROM gold.fait_passage f
JOIN gold.dim_gare g ON f.ptcar_no = g.ptcar_no
WHERE f.a_l_heure IS NOT NULL
GROUP BY g.ptcar_no, g.nom_nl, g.est_jonction_nord_midi

-- COMMAND ----------

-- Question 2 : les relations et les types de train, au terminus.
CREATE OR REPLACE VIEW gold.v_ponctualite_relation
COMMENT 'Ponctualité au terminus par relation et par type de train'
AS
SELECT
    r.relation,
    r.type_train,
    o.operateur_nom,
    count(*) AS trains,
    round(100 * avg(CAST(f.a_l_heure AS DOUBLE)), 2) AS pct_a_l_heure,
    round(avg(f.retard_arrivee_min), 2) AS retard_moyen_min
FROM gold.fait_passage f
JOIN gold.dim_relation r ON f.relation_cle = r.relation_cle
JOIN gold.dim_operateur o ON f.operateur_code = o.operateur_code
WHERE f.est_terminus AND f.a_l_heure IS NOT NULL
GROUP BY r.relation, r.type_train, o.operateur_nom

-- COMMAND ----------

-- Question 3 : l'heure et le type de jour.
CREATE OR REPLACE VIEW gold.v_ponctualite_heure
COMMENT 'Ponctualité en arrivant, par heure prévue et type de jour'
AS
SELECT
    h.heure,
    h.tranche,
    h.ordre_tranche,
    d.type_jour,
    count(*) AS arrivees,
    round(100 * avg(CAST(f.a_l_heure AS DOUBLE)), 2) AS pct_a_l_heure,
    round(avg(f.retard_arrivee_min), 2) AS retard_moyen_min
FROM gold.fait_passage f
JOIN gold.dim_heure h ON f.minute_cle = h.minute_cle
JOIN gold.dim_date d ON f.date_cle = d.date_cle
WHERE f.a_l_heure IS NOT NULL
GROUP BY h.heure, h.tranche, h.ordre_tranche, d.type_jour

-- COMMAND ----------

-- Question 4 : le retard le long du trajet, arrêt par arrêt, du 2e au 20e point (le 20e regroupe les suivants).
-- Au 1er point, seuls les trains entrés depuis un pays voisin ont une arrivée mesurée : ils fausseraient le début de la courbe.
CREATE OR REPLACE VIEW gold.v_propagation
COMMENT 'Retard moyen et retard pris en route selon le rang du point dans le trajet, à partir du 2e point'
AS
SELECT
    least(f.rang_arret, 20) AS rang_arret,
    count(*) AS arrivees,
    round(avg(f.retard_arrivee_min), 2) AS retard_moyen_min,
    round(avg(f.retard_pris_en_ligne_min), 3) AS retard_pris_en_ligne_min,
    round(avg(f.retard_pris_a_quai_min), 3) AS retard_pris_a_quai_min
FROM gold.fait_passage f
WHERE f.retard_arrivee_min IS NOT NULL
  AND f.rang_arret >= 2
GROUP BY least(f.rang_arret, 20)

-- COMMAND ----------

-- Question 5 : la jonction Nord-Midi, comparée au reste du réseau.
CREATE OR REPLACE VIEW gold.v_jonction_nord_midi
COMMENT 'Retard pris en route sur la jonction Nord-Midi et sur le reste du réseau'
AS
SELECT
    CASE WHEN g.est_jonction_nord_midi THEN 'jonction Nord-Midi' ELSE 'reste du réseau' END AS zone,
    count(*) AS arrivees,
    round(100 * avg(CAST(f.a_l_heure AS DOUBLE)), 2) AS pct_a_l_heure,
    round(avg(f.retard_pris_en_ligne_min), 3) AS retard_pris_en_ligne_min,
    round(avg(f.retard_pris_a_quai_min), 3) AS retard_pris_a_quai_min
FROM gold.fait_passage f
JOIN gold.dim_gare g ON f.ptcar_no = g.ptcar_no
WHERE f.retard_arrivee_min IS NOT NULL
GROUP BY CASE WHEN g.est_jonction_nord_midi THEN 'jonction Nord-Midi' ELSE 'reste du réseau' END

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Un premier coup d'œil
-- MAGIC
-- MAGIC Sur août et septembre 2026, la ponctualité au terminus doit retrouver à peu près les 92,8 % et 91,1 % de l'exploration : seules 67 lignes sont parties en quarantaine entre-temps.

-- COMMAND ----------

SELECT * FROM gold.v_ponctualite_mois ORDER BY annee_mois

-- COMMAND ----------

SELECT * FROM gold.v_jonction_nord_midi

-- COMMAND ----------

SELECT * FROM gold.v_ponctualite_gare WHERE arrivees >= 500 ORDER BY pct_a_l_heure LIMIT 15

-- COMMAND ----------

-- Les codes de train que le notebook ne connaît pas encore : le résultat doit être vide.
SELECT r.code_type,
       count(*) AS trains,
       array_join(collect_set(f.operateur_code), ', ') AS operateurs,
       array_join(slice(array_sort(collect_set(r.relation)), 1, 6), ', ') AS exemples
FROM gold.fait_passage f
JOIN gold.dim_relation r ON f.relation_cle = r.relation_cle
WHERE f.est_terminus AND r.type_train = 'Autre'
GROUP BY r.code_type
ORDER BY trains DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## La suite
-- MAGIC
-- MAGIC Le tableau de bord AI/BI se branche sur ces vues. Le job `ponctualite_trains_belges` enchaîne les notebooks 02, 03 et 04, puis rafraîchit le tableau de bord. Il démarre dès qu'un nouveau fichier arrive dans le volume.
