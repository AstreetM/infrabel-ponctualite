-- Les codes de train que dim_relation ne connaît pas encore. Le test ne bloque pas le job :
-- il avertit, pour qu'on ajoute le code à la liste avant qu'il ne pèse dans les chiffres.
{{ config(severity='warn') }}

SELECT r.code_type,
       count(*) AS trains,
       array_join(collect_set(f.operateur_code), ', ') AS operateurs,
       array_join(slice(array_sort(collect_set(r.relation)), 1, 6), ', ') AS exemples
FROM {{ ref('fait_passage') }} f
JOIN {{ ref('dim_relation') }} r ON f.relation_cle = r.relation_cle
WHERE f.est_terminus AND r.type_train = 'Autre'
GROUP BY r.code_type
