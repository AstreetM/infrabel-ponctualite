-- Question 2 : les points du réseau. La médiane résiste mieux que la moyenne aux retards de plusieurs heures.
SELECT
    g.ptcar_no,
    g.nom_nl,
    g.est_jonction_nord_midi,
    count(*) AS arrivees,
    round(100 * avg(CAST(f.a_l_heure AS DOUBLE)), 2) AS pct_a_l_heure,
    round(avg(f.retard_arrivee_min), 2) AS retard_moyen_min,
    round(percentile_approx(f.retard_arrivee_min, 0.5), 2) AS retard_median_min,
    round(avg(f.retard_pris_en_ligne_min), 2) AS retard_pris_en_ligne_moyen_min
FROM {{ ref('fait_passage') }} f
JOIN {{ ref('dim_gare') }} g ON f.ptcar_no = g.ptcar_no
WHERE f.a_l_heure IS NOT NULL
GROUP BY g.ptcar_no, g.nom_nl, g.est_jonction_nord_midi
