-- Le fait garde chaque passage du silver, ni plus ni moins. Une ligne en sortie fait échouer le test.
SELECT fait.lignes AS lignes_fait, silver.lignes AS lignes_silver
FROM (SELECT count(*) AS lignes FROM {{ ref('fait_passage') }}) fait
CROSS JOIN (SELECT count(*) AS lignes FROM {{ source('silver', 'passages') }}) silver
WHERE fait.lignes <> silver.lignes
