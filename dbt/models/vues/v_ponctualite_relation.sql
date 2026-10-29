-- Question 2 : les relations et les types de train, au terminus.
SELECT
    r.relation,
    r.type_train,
    o.operateur_nom,
    count(*) AS trains,
    round(100 * avg(CAST(f.a_l_heure AS DOUBLE)), 2) AS pct_a_l_heure,
    round(avg(f.retard_arrivee_min), 2) AS retard_moyen_min
FROM {{ ref('fait_passage') }} f
JOIN {{ ref('dim_relation') }} r ON f.relation_cle = r.relation_cle
JOIN {{ ref('dim_operateur') }} o ON f.operateur_code = o.operateur_code
WHERE f.est_terminus AND f.a_l_heure IS NOT NULL
GROUP BY r.relation, r.type_train, o.operateur_nom
