{{ config(materialized='table', schema='dw_ds', tags=['daily']) }}

{% set items = adapter.get_columns_in_relation(ref('qs_pa_inference')) %}
{% set except_col_names=['SALESFORCE_ACCOUNT_ID'
                            ,'DATE_WEEK'
                            ,'DATA_AS_OF'
                            ,'FIRST_PROCORE_START_DATE'
                            ,'MARKET_SERVED'
                            ,'INSTALLED_INTEGRATIONS'
                            ,'SERVICE_START_DATE'
                            ,'SERVICE_END_DATE'
                            ,'ACV'
                            ,'SALESFORCE_ACCOUNT_ID'
                            ,'REGION_SEGEMENT'
                            ,'COMPANY_SEGMENT'
                            ,'COMPANY_TYPE_TIER_1'
                            ,'COMPANY_TYPE'
                            ,'ACV_RANGE'
                            ,'DOES_HAVE_PRODUCT'] %}

with benchmark as(
        select
             {% for col in items -%}
                {% if  col.name not in except_col_names -%}
                    AVG("{{col.name}}") as "{{col.name}}_benchmark",
                {% endif %}
            {%- endfor %}
            COMPANY_TYPE,
            COMPANY_SEGMENT,
            date_week
        from {{ref('qs_pa_inference')}}
        where does_have_product
        group by company_type, COMPANY_SEGMENT, date_week
)

SELECT
    s.SALESFORCE_ACCOUNT_ID
    , s.DATE_WEEK
{% for col in items -%}
    {% if col.name not in except_col_names -%}

        , b."{{col.name}}_benchmark" as "{{col.name}}_benchmark"

    {%- endif %}
{%- endfor %}
FROM {{ref('qs_pa_inference')}} s
LEFT JOIN  {{ref('company_cohorts')}} c on
    c.company_type = s.company_type
    and c.company_segment = s.company_segment
LEFT JOIN benchmark as b on
    s.company_type = b.company_type
    and s.COMPANY_SEGMENT = b.COMPANY_SEGMENT
    and s.date_week = b.date_week
WHERE SALESFORCE_ACCOUNT_ID is not null
