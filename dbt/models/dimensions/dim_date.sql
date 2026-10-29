-- Un jour par ligne, du premier au dernier jour du silver, avec les jours fériés belges du seed jours_feries.
WITH bornes AS (
    SELECT min(date_depart) AS debut, max(date_depart) AS fin
    FROM {{ source('silver', 'passages') }}
),
jours AS (
    SELECT explode(sequence(debut, fin, INTERVAL 1 DAY)) AS date_jour
    FROM bornes
)
SELECT
    {{ cle_date('j.date_jour') }} AS date_cle,
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
LEFT JOIN {{ ref('jours_feries') }} f ON j.date_jour = f.date_ferie
