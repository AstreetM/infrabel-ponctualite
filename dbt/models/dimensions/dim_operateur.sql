-- Un opérateur ferroviaire par ligne, avec un nom lisible.
SELECT DISTINCT
    operateur AS operateur_code,
    CASE operateur
        WHEN 'SNCB/NMBS' THEN 'SNCB'
        WHEN 'EURSLEEPER' THEN 'European Sleeper'
        WHEN 'THI-FACT' THEN 'Eurostar (ex-Thalys)'
        ELSE operateur
    END AS operateur_nom
FROM {{ source('silver', 'passages') }}
