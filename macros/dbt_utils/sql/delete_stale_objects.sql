{% macro athena__delete_stale_ctas(schema, dry_run=True, except_names='') %}
  {{ return (_athena__delete_stale_objects(schema, dry_run, except_names, 'ctas_.*_[0-9]+', True))}}
{% endmacro %}

{% macro athena__delete_stale_views(schema, dry_run=True, except_names='') %}
  {{ return (_athena__delete_stale_objects(schema, dry_run, '{},ctas_.*_[0-9]+'.format(except_names), '', False))}}
{% endmacro %}

{% macro athena__delete_stale_objects(schema, dry_run=True, except_names='') %}
  {{ athena__delete_stale_views(schema, dry_run, except_names)}}
  {{ athena__delete_stale_ctas(schema, dry_run, except_names)}}
{% endmacro %}

{% macro _athena__delete_stale_objects(schema, dry_run=True, except_names='', include_names='', skip_latest_entity=False) %}
  {% if (schema is not string and schema is not iterable) or schema is mapping or schema|length <= 0 %}
    {% do exceptions.raise_compiler_error('"schema" must be a string or a list') %}
  {% endif %}
  
  {% set except_names_list = except_names.split(',') %}
  {% set include_names_list = include_names.split(',') %}

  {% set query %}
    SELECT current.schema_name,
           current.ref_name,
		   current.table_type,
		   current.ref_base_name
    FROM (
      SELECT table_schema AS schema_name,
             table_name  AS ref_name,
			 CASE WHEN regexp_like(table_name, '(?i)\Actas_.*_[0-9]+\Z') THEN substr(table_name, 1, length(table_name) - 14) ELSE table_name END AS ref_base_name,
             {{ dbt_utils.get_table_types_sql() }}
      FROM {{ target.database }}.information_schema.tables
      WHERE table_schema IN (
	    {%- if schema is string -%}
          '{{ schema }}'
        {%- elif schema is iterable and (var is not string and var is not mapping) -%}
          {%- for s in schema -%}
            '{{ s }}'{% if not loop.last %},{% endif %}
          {%- endfor -%}
        {%- endif -%}
      )
	  {%- for e in except_names_list -%}
	    {%- if e | length > 0 -%}
		  AND not regexp_like(table_name, '(?i)\A{{e}}\Z')
		{%- endif -%}
	  {%- endfor-%}
	  {%- for i in include_names_list -%}
	    {%- if i | length > 0 -%}
		  AND regexp_like(table_name, '(?i)\A{{i}}\Z')
		{%- endif -%}
	  {%- endfor-%}
	  
	) as current
    LEFT JOIN (
      {%- for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") | list
                    + graph.nodes.values() | selectattr("resource_type", "equalto", "seed")  | list %}
        SELECT
        '{{node.schema}}' AS schema_name
         ,'{{node.name}}' AS ref_name
        {% if not loop.last %} UNION ALL {% endif %}
      {%- endfor %}
    ) AS desired on desired.schema_name = current.schema_name
                and desired.ref_name    = current.ref_name
    WHERE desired.ref_name is null
	ORDER BY ref_name DESC
  {% endset %}
  {{ log("Delete Stale Objects debug query: " ~ query)}}
  {%- set result = run_query(query) -%}
  {%- set processed_ctas = [] -%}
  {% if result %}
      {%- for to_delete in result -%}
	    {%- if skip_latest_entity and to_delete[3] not in processed_ctas -%}
		  {{ log("First time met object " ~ to_delete[3] ~ ", skipping drop of " ~ to_delete[1], True)}}
		  {{ processed_ctas.append(to_delete[3]) }}
		{%- else -%}
          {%- if dry_run -%}
            {%- do log('To be dropped: ' ~ to_delete[2] ~ ' ' ~ to_delete[0] ~ '.' ~ to_delete[1], True) -%}
          {%- else -%}
            {%- do log('Dropping ' ~ to_delete[2] ~ ' ' ~ to_delete[0] ~ '.' ~ to_delete[1], True) -%}
            {% set drop_command = 'DROP ' ~ to_delete[2] ~ ' IF EXISTS ' ~ to_delete[0] ~ '.' ~ to_delete[1] ~ ';' %}
            {% do run_query(drop_command) %}
            {%- do log('Dropped ' ~ to_delete[2] ~ ' ' ~ to_delete[0] ~ '.' ~ to_delete[1], True) -%}
          {%- endif -%}
		{%- endif -%}
      {%- endfor -%}
  {% else %}
    {% do log('No orphan tables to clean.', True) %}
  {% endif %}
{% endmacro %}