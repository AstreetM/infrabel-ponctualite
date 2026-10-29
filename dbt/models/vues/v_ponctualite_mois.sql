-- Question 1 : la ponctualité au terminus, mois par mois, comparable aux chiffres officiels d'Infrabel.
SELECT
    d.annee_mois,
    d.annee,
    d.mois,
    d.nom_mois,
    count(*) AS trains,
    round(100 * avg(CAST(f.a_l_heure AS DOUBLE)), 2) AS pct_a_l_heure,
    round(avg(f.retard_arrivee_min), 2) AS retard_moyen_min
FROM {{ ref('fait_passage') }} f
JOIN {{ ref('dim_date') }} d ON f.date_cle = d.date_cle
WHERE f.est_terminus AND f.a_l_heure IS NOT NULL
GROUP BY d.annee_mois, d.annee, d.mois, d.nom_mois
