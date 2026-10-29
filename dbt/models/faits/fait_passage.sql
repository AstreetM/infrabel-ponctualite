{# Les heures du silver sont des heures belges lues sans conversion : on garde le même fuseau pour en extraire l'heure. #}
{{ config(pre_hook="SET TIME ZONE 'Etc/UTC'") }}

-- Une ligne par passage du silver. Les points d'un même train, un même jour, sont classés par heure prévue :
-- c'est ce qui donne le rang de l'arrêt, le premier et le dernier point du trajet, et le retard pris en route.
WITH ordonnes AS (
    SELECT
        p.*,
        coalesce(p.arrivee_prevue, p.depart_prevu) AS heure_prevue,
        row_number() OVER trajet AS rang_arret,
        count(*) OVER (PARTITION BY p.date_depart, p.train_no) AS nb_arrets,
        lag(p.retard_depart_min) OVER trajet AS retard_depart_precedent_min
    FROM {{ source('silver', 'passages') }} p
    WINDOW trajet AS (
        PARTITION BY p.date_depart, p.train_no
        ORDER BY coalesce(p.arrivee_prevue, p.depart_prevu), p.id_passage
    )
)
SELECT
    id_passage,
    {{ cle_date('date_depart') }} AS date_cle,
    hour(heure_prevue) * 60 + minute(heure_prevue) AS minute_cle,
    ptcar_no,
    {{ cle_relation('relation', 'sens_relation') }} AS relation_cle,
    operateur AS operateur_code,
    train_no,
    date_depart,
    arrivee_prevue,
    arrivee_reelle,
    depart_prevu,
    depart_reel,
    retard_arrivee_min,
    retard_depart_min,
    CASE WHEN retard_arrivee_min IS NOT NULL THEN retard_arrivee_min < 6 END AS a_l_heure,
    rang_arret,
    nb_arrets,
    rang_arret = 1 AS est_origine,
    rang_arret = nb_arrets AS est_terminus,
    retard_arrivee_min - retard_depart_precedent_min AS retard_pris_en_ligne_min,
    retard_depart_min - retard_arrivee_min AS retard_pris_a_quai_min,
    retard_arrivee_incoherent OR retard_depart_incoherent AS retard_incoherent
FROM ordonnes
