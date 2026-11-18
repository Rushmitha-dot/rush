{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}

{% test at_least(model, column_name, value) %}
select * from {{ model }} where {{ column_name }} < {{ value }}
{% endtest %}

{% test between_values(model, column_name, low, high) %}
select * from {{ model }} where {{ column_name }} < {{ low }} or {{ column_name }} > {{ high }}
{% endtest %}
