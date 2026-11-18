{#
  Limits an incremental model to the nights in this run. Dagster passes the
  partition range as vars; a full refresh or a plain `dbt build` takes all nights.
#}
{% macro night_filter(column='night') -%}
    {%- if is_incremental() and var('first_night') -%}
    {{ column }} between date '{{ var("first_night") }}' and date '{{ var("last_night") }}'
    {%- else -%}
    true
    {%- endif -%}
{%- endmacro %}
