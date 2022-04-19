{%- macro athena__get_tables_by_pattern_sql(schema_pattern, table_pattern, exclude='', database=target.database) -%}

{% set table_schema_like_str = "regexp_like({}, '(?i)\\A{}\\Z')".format("table_schema", schema_pattern) %}
{% set table_name_like_str = "regexp_like({}, '(?i)\\A{}\\Z')".format("table_name", table_pattern) %}
{% set table_name_not_like_str = "not regexp_like({}, '(?i)\\A{}\\Z')".format("table_name", exclude) %}

        select distinct
            table_schema as "table_schema",
            table_name as "table_name",
            {{ dbt_utils.get_table_types_sql() }}
        from {{ database }}.information_schema.tables
        where {{ table_schema_like_str }}
        and {{ table_name_like_str }}
        and {{ table_name_not_like_str }}

{%- endmacro -%}
