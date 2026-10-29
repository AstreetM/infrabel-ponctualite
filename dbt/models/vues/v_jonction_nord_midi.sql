-- Question 5 : la jonction Nord-Midi, comparée au reste du réseau.
SELECT
    CASE WHEN g.est_jonction_nord_midi THEN 'jonction Nord-Midi' ELSE 'reste du réseau' END AS zone,
    count(*) AS arrivees,
    round(100 * avg(CAST(f.a_l_heure AS DOUBLE)), 2) AS pct_a_l_heure,
    round(avg(f.retard_pris_en_ligne_min), 3) AS retard_pris_en_ligne_min,
    round(avg(f.retard_pris_a_quai_min), 3) AS retard_pris_a_quai_min
FROM {{ ref('fait_passage') }} f
JOIN {{ ref('dim_gare') }} g ON f.ptcar_no = g.ptcar_no
WHERE f.retard_arrivee_min IS NOT NULL
GROUP BY CASE WHEN g.est_jonction_nord_midi THEN 'jonction Nord-Midi' ELSE 'reste du réseau' END
