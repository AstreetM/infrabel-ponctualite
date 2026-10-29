-- Chaque train a un seul terminus par jour : c'est là qu'Infrabel mesure la ponctualité.
SELECT date_depart, train_no, count_if(est_terminus) AS terminus
FROM {{ ref('fait_passage') }}
GROUP BY date_depart, train_no
HAVING count_if(est_terminus) <> 1
