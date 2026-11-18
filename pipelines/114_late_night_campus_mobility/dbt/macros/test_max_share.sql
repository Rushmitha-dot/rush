{# fails when more than max_share of rows match the condition #}
{% test max_share(model, condition, max_share) %}
select share from (
    select count(*) filter (where {{ condition }}) / nullif(count(*), 0) as share from {{ model }}
) where share > {{ max_share }}
{% endtest %}
