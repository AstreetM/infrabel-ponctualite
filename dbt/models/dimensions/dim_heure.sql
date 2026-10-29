-- Une minute de la journée par ligne, soit 1 440 lignes. Les pointes suivent ce que l'exploration a montré :
-- le retard monte de 7 h à 9 h, puis en fin d'après-midi.
SELECT
    m AS minute_cle,
    m DIV 60 AS heure,
    m % 60 AS minute,
    format_string('%02d:%02d', m DIV 60, m % 60) AS heure_minute,
    CASE
        WHEN m DIV 60 IN (7, 8) THEN 'pointe du matin'
        WHEN m DIV 60 BETWEEN 9 AND 15 THEN 'journée'
        WHEN m DIV 60 IN (16, 17, 18) THEN 'pointe du soir'
        WHEN m DIV 60 BETWEEN 19 AND 22 THEN 'soirée'
        ELSE 'nuit et petit matin'
    END AS tranche,
    CASE
        WHEN m DIV 60 IN (7, 8) THEN 2
        WHEN m DIV 60 BETWEEN 9 AND 15 THEN 3
        WHEN m DIV 60 IN (16, 17, 18) THEN 4
        WHEN m DIV 60 BETWEEN 19 AND 22 THEN 5
        ELSE 1
    END AS ordre_tranche,
    m DIV 60 IN (7, 8, 16, 17, 18) AS est_pointe
FROM (SELECT explode(sequence(0, 1439)) AS m)
