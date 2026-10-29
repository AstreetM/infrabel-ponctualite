{#
  Les clés partagées entre la table de faits et ses dimensions. Elles sont calculées au même endroit,
  pour qu'une jointure ne puisse jamais échouer parce qu'un des deux côtés aurait changé sa formule.
#}

{# La clé d'un jour : un entier au format aaaammjj, comme le veut l'usage dans un entrepôt. #}
{% macro cle_date(colonne) -%}
    CAST(date_format({{ colonne }}, 'yyyyMMdd') AS INT)
{%- endmacro %}

{# La clé d'une relation et de son sens : une empreinte SHA-256, qui reste valable même si un des deux est vide. #}
{% macro cle_relation(relation, sens_relation) -%}
    sha2(concat_ws('|', coalesce({{ relation }}, ''), coalesce({{ sens_relation }}, '')), 256)
{%- endmacro %}
