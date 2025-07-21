# Note this prompt template is only used for a naive baseline implementation for LLM-based SQL template generation
# For all the prompts used in SQLBarber, please refer to src/sqlbarber/template_generator.py

SQL_GENERATION_TEMPLATE = [
"""
Task:
Using the provided DB_INFO, generate {num_of_sql} SQL templates to query data from the table. 
The generated template should include placeholders for predicate values since the specific values for the predicates are unknown. 

DB_INFO:
{db_info}

SQL Requirement:
Each query template should be designed to extract meaningful insights such as retrieving specific records, filtering data, or performing aggregations. 
{semantic_requirement}

Note:
1. Predicate values (the dynamic values that will be inserted for filtering) should be wrapped in double curly braces with single quotes like `'{{{{}}}}'`.
2. Ensure that all predicate values wrapped in double curly braces are enclosed in single quotes, e.g., `'{{{{real_table_name.real_column_name}}}}'`.
3. Single curly braces are not allowed. Use double curly braces with single quotes `'{{{{real_table_name.real_column_name}}}}'`.
4. Table names, column names, and JOIN conditions should be written directly without any curly braces or quotes. Double curly braces with single quotes are only for placeholders where predicate values will be inserted.
5. For predicates with both lower and upper bounds, use `'{{{{real_table_name.real_column_name_start}}}}'` and `'{{{{real_table_name.real_column_name_end}}}}'` to represent the placeholder values, but do not wrap the actual column names in curly braces.
6. The table names and column names should exactly match those in the database. Include both real table name and column name like `'{{{{real_table_name.real_column_name_end}}}}'`.

Return the results of each query in JSON format:
{{
    "query1": "SELECT ...",
    "query2": "SELECT ...",
    "query3": "SELECT ..."
}}

SQL Queries:
"""
]