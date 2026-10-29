-- Un point du réseau par ligne. L'exploration a montré que chaque numéro garde un seul nom ;
-- par prudence, le nom retenu est le plus récent. Les cinq points de la jonction Nord-Midi sont marqués.
SELECT
    ptcar_no,
    max_by(point_arret, _charge_le) AS nom_nl,
    max_by(point_arret, _charge_le) RLIKE '^BRUSSEL-(NOORD|CONGRES|CENTRAAL|KAPELLEKERK|ZUID)$' AS est_jonction_nord_midi
FROM {{ source('silver', 'passages') }}
GROUP BY ptcar_no
