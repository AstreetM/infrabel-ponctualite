-- Question 4 : le retard le long du trajet, arrêt par arrêt, du 2e au 20e point (le 20e regroupe les suivants).
-- Au 1er point, seuls les trains entrés depuis un pays voisin ont une arrivée mesurée : ils fausseraient le début de la courbe.
SELECT
    least(f.rang_arret, 20) AS rang_arret,
    count(*) AS arrivees,
    round(avg(f.retard_arrivee_min), 2) AS retard_moyen_min,
    round(avg(f.retard_pris_en_ligne_min), 3) AS retard_pris_en_ligne_min,
    round(avg(f.retard_pris_a_quai_min), 3) AS retard_pris_a_quai_min
FROM {{ ref('fait_passage') }} f
WHERE f.retard_arrivee_min IS NOT NULL
  AND f.rang_arret >= 2
GROUP BY least(f.rang_arret, 20)
