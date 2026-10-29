-- Question 3 : l'heure et le type de jour.
SELECT
    h.heure,
    h.tranche,
    h.ordre_tranche,
    d.type_jour,
    count(*) AS arrivees,
    round(100 * avg(CAST(f.a_l_heure AS DOUBLE)), 2) AS pct_a_l_heure,
    round(avg(f.retard_arrivee_min), 2) AS retard_moyen_min
FROM {{ ref('fait_passage') }} f
JOIN {{ ref('dim_heure') }} h ON f.minute_cle = h.minute_cle
JOIN {{ ref('dim_date') }} d ON f.date_cle = d.date_cle
WHERE f.a_l_heure IS NOT NULL
GROUP BY h.heure, h.tranche, h.ordre_tranche, d.type_jour
