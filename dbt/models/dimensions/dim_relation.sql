-- Une relation et son sens par ligne, avec le type de train déduit du code (IC, L, S, P…).
-- Un code inconnu tombe dans « Autre » : le test aucun_code_de_train_inconnu le signale.
SELECT
    {{ cle_relation('relation', 'sens_relation') }} AS relation_cle,
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
    FROM {{ source('silver', 'passages') }}
)
