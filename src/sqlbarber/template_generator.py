import os
import datetime
import json
import random
import sqlparse
import math
import re
import concurrent.futures
from pathlib import Path

class NaiveSQLTemplateGenerator:
    def __init__(self, task_name, db_controller, llm, folder_path=f"{Path(__file__).resolve().parents[2]}/outputs/final/sql_template"):
        self.db_controller = db_controller
        self.llm = llm
        self.folder_path = folder_path + f"/{task_name}"
        self.db_info = self.get_database_info()

    def get_database_info(self):
        """
            Extract database table-level and column-level information.
        """
        db_controller = self.db_controller
        # Step 1: Get table names 
        table_query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public';
        """
        tables = db_controller.execute_sql(table_query)["result"]
        if not tables:
            return "No tables found in the database."
        
        result_str = "Database Information:\n"
        result_str += "-" * 80 + "\n"
        
        # Step 2: Iterate each table to get table information
        for table in tables:
            table_name = table[0]

            # Step 3: Get table size
            size_query = f"""
            SELECT pg_size_pretty(pg_total_relation_size('{table_name}'));
            """
            table_size = db_controller.execute_sql(size_query)["result"]
            table_size_str = table_size[0][0] if table_size else 'Unknown'

            # Step 4: Get the number of rows in a table
            row_count_query = f"SELECT COUNT(*) FROM {table_name};"
            row_count_result = db_controller.execute_sql(row_count_query)["result"]
            row_count = row_count_result[0][0] if row_count_result else 'Unknown'

            result_str += f"Table: {table_name}, Size: {table_size_str}, Row Count: {row_count}\n"
            
            # Step 5: Get column name, type, and NULL constraint
            column_query = f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = '{table_name}';
            """
            columns = db_controller.execute_sql(column_query)["result"]
            
            result_str += f"{'Column Name':<30} {'Data Type':<20} {'Unique Values':<15} {'NOT NULL':<10}\n"
            result_str += "-" * 80 + "\n"
            
            # Step 6: Get the unique number of values in a column
            for column in columns:
                column_name, data_type, is_nullable = column
                not_null = 'Yes' if is_nullable == 'NO' else 'No'
                unique_count_query = f"""
                SELECT COUNT(DISTINCT {column_name})
                FROM {table_name};
                """
                unique_count_result = db_controller.execute_sql(unique_count_query)["result"]
                unique_count = unique_count_result[0][0] if unique_count_result else 'Unknown'
                
                result_str += f"{column_name:<30} {data_type:<20} {unique_count:<15} {not_null:<10}\n"
            
            # Get primary key information
            pk_query = f"""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = '{table_name}' AND tc.constraint_type = 'PRIMARY KEY';
            """
            primary_keys = db_controller.execute_sql(pk_query)["result"]
            if primary_keys:
                primary_key_cols = ', '.join([pk[0] for pk in primary_keys])
                result_str += f"Primary Key: {primary_key_cols}\n"
            else:
                result_str += "Primary Key: None\n"
            
            # Get foreign key information
            fk_query = f"""
            SELECT kcu.column_name, ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu
            ON ccu.constraint_name = tc.constraint_name
            WHERE tc.table_name = '{table_name}' AND tc.constraint_type = 'FOREIGN KEY';
            """
            foreign_keys = db_controller.execute_sql(fk_query)["result"]
            if foreign_keys:
                for fk in foreign_keys:
                    result_str += f"Foreign Key: {fk[0]} -> {fk[1]}({fk[2]})\n"
            else:
                result_str += "Foreign Key: None\n"
            
            # Get index information
            index_query = f"""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE tablename = '{table_name}';
            """
            indexes = db_controller.execute_sql(index_query)["result"]
            if indexes:
                result_str += "Indexes:\n"
                for index in indexes:
                    result_str += f"{index[0]}: {index[1]}\n"
            else:
                result_str += "Indexes: None\n"
            
            result_str += "\n" + "-" * 80 + "\n"
        
        return result_str
    
    def generate_prompt(self, prompt_template, num_of_templates, semantic_requirement=None):
        """
            Generate prompt based on user-provided placeholders, return the prompt.
        """
        if semantic_requirement is None:
            prompt = prompt_template[0].format(num_of_sql=num_of_templates, db_info=self.db_info)
        else:
            prompt = prompt_template[0].format(num_of_sql=num_of_templates, db_info=self.db_info, semantic_requirement=semantic_requirement)

        return prompt

    def generate_sql_template(self, prompt, semantic_requirement=None):
        """
            Call llm to create SQL templates based on the provided prompt and write the result to the specified folder_path.
        """
        llm = self.llm
        templates = llm.get_GPT_response_json(prompt, json_format=True)

        folder_path = self.folder_path
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        # Correct the logic to look for template_<number>.sql files
        existing_files = [f for f in os.listdir(folder_path) if f.startswith('template_') and f.endswith('.sql')]
        existing_numbers = sorted([int(f.split('_')[1].split('.')[0]) for f in existing_files if f.split('_')[1].split('.')[0].isdigit()])

        # Start numbering the new templates from the highest existing number + 1, or 1 if no files exist
        start_number = existing_numbers[-1] + 1 if existing_numbers else 1

        # Generate and save each template with a file name template_<i>.sql
        for i, (key, sql_template) in enumerate(templates.items(), start=start_number):
            file_name = f"template_{i}.sql"
            file_path = os.path.join(folder_path, file_name)

            # Add meta information as comments
            creation_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            meta_info = (
                f"-- SQL Template Metadata\n"
                f"-- Creation Time: {creation_time}\n"
                f"-- LLM Model: {self.llm.model}\n"
                f"-- Semantic Requirement: {semantic_requirement if semantic_requirement else 'None'}\n"
                f"--\n"
            )

            formatted_sql = sqlparse.format(sql_template, reindent=True, keyword_case="upper")

            with open(file_path, 'w') as f:
                f.write(meta_info + formatted_sql)

    def load_sql_templates(self):
        """
            Load all template_i.sql files from the given folder into a list.
        """
        folder_path = self.folder_path

        template_ids = []
        templates = []
        # Iterate over all files in the folder that match template_i.sql pattern
        for filename in os.listdir(folder_path):
            if filename.startswith('template_') and filename.endswith('.sql'):
                file_path = os.path.join(folder_path, filename)
                
                # Read the content of each template file
                with open(file_path, 'r') as file:
                    sql_template = file.read()
                    template_ids.append(f"{filename.split('.')[0]}")
                    templates.append(sql_template)
        
        return template_ids, templates

class AdvancedSQLTemplateGenerator:
    def __init__(self, task_name, db_controller, llm, folder_path=f"{Path(__file__).resolve().parents[2]}/outputs/final/sql_template"):
        self._root = Path(__file__).resolve().parents[2]
        self.task_name = task_name
        self.db_controller = db_controller
        self.llm = llm
        self.folder_path = os.path.join(folder_path, task_name)
        self.joinable_path_path = f"{self._root}/outputs/intermediate/db_meta_info/{self.task_name}/joinable_path.json"
        self.schema_path = f"{self._root}/outputs/intermediate/db_meta_info/{self.task_name}/schema.json"
        self.constraint_path = f"{self._root}/benchmark/template_specification"

        # Log file setup
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
        self.log_file = os.path.join(f"{self._root}/outputs/intermediate/logs/{self.task_name}_{timestamp}", f"llm.log")
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)

        self.log("Initializing AdvancedSQLTemplateGenerator.")

        # Step 1: Fetch and store database schema
        self.db_schema = self.fetch_database_schema()

        # Step 2: Generate joinable paths using LLM
        self.joinable_paths = self.generate_joinable_paths()

        # Initialize templates info
        self.templates_info = []

    def log(self, message):
        """Append message to log file with timestamp."""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.log_file, "a") as log_file:
            log_file.write(f"[{timestamp}] {message}\n")

    def fetch_database_schema(self):
        """
        Fetch and store database schema information in a structured format.
        (Includes table size, row count, column uniqueness counts, PK/FK info, and indexes.)
        """

        schema_path = self.schema_path 
        if os.path.exists(schema_path):
            with open(schema_path, "r", encoding="utf-8") as f:
                schema = json.load(f)
        else:
            schema = None  
        
        if schema is not None:
            return schema

        self.log("Starting database schema extraction.")
        db_controller = self.db_controller
        schema = {'tables': {}}

        # Step 1: Get table names
        table_query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public';
        """
        tables = db_controller.execute_sql(table_query)["result"]

        if not tables:
            self.log("No tables found in the database.")
            return schema

        for table in tables:
            table_name = table[0]
            schema['tables'][table_name] = {}
            table_info = schema['tables'][table_name]

            # --- TABLE SIZE ---
            size_query = f"""
            SELECT pg_size_pretty(pg_total_relation_size('{table_name}'));
            """
            size_result = db_controller.execute_sql(size_query)["result"]
            table_info['size'] = size_result[0][0] if size_result else 'Unknown'

            # --- ROW COUNT ---
            row_count_query = f"SELECT COUNT(*) FROM {table_name};"
            row_count_result = db_controller.execute_sql(row_count_query)["result"]
            table_info['row_count'] = row_count_result[0][0] if row_count_result else 'Unknown'

            # --- COLUMN INFORMATION ---
            column_query = f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = '{table_name}';
            """
            columns = db_controller.execute_sql(column_query)["result"]
            table_info['columns'] = {}

            for column in columns:
                column_name, data_type, is_nullable = column
                # Unique values in the column
                unique_count_query = f"SELECT COUNT(DISTINCT {column_name}) FROM {table_name};"
                unique_count_result = db_controller.execute_sql(unique_count_query)["result"]
                unique_count = unique_count_result[0][0] if unique_count_result else 'Unknown'

                table_info['columns'][column_name] = {
                    'data_type': data_type,
                    'is_nullable': (is_nullable == 'YES'),
                    'unique_values': unique_count
                }

            # --- PRIMARY KEYS ---
            pk_query = f"""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = '{table_name}' AND tc.constraint_type = 'PRIMARY KEY';
            """
            primary_keys = db_controller.execute_sql(pk_query)["result"]
            table_info['primary_keys'] = [pk[0] for pk in primary_keys] if primary_keys else []

            # --- FOREIGN KEYS ---
            fk_query = f"""
            SELECT
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM
                information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_name = '{table_name}';
            """
            foreign_keys = db_controller.execute_sql(fk_query)["result"]
            table_info['foreign_keys'] = []
            for fk in foreign_keys:
                table_info['foreign_keys'].append({
                    'column': fk[0],
                    'references': {
                        'table': fk[1],
                        'column': fk[2],
                    }
                })

            # --- INDEXES ---
            index_query = f"""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE tablename = '{table_name}';
            """
            indexes = db_controller.execute_sql(index_query)["result"]
            table_info['indexes'] = []
            for index in indexes:
                table_info['indexes'].append({
                    'name': index[0],
                    'definition': index[1]
                })

        # Save schema to JSON file
        os.makedirs(os.path.dirname(schema_path), exist_ok=True)
        with open(schema_path, 'w') as f:
            json.dump(schema, f, indent=4)

        self.log(f"Database schema saved to {schema_path}")
        return schema

    def generate_joinable_paths(self):
        """
        Generate all possible joinable paths using LLM.
        """
        self.log("Starting joinable path generation using LLM.")

        schema = self.db_schema
        tables = schema['tables']

        # Build the prompt for LLM
        prompt = """
Given the following database schema, generate all possible joinable paths based on foreign key relationships.
If the number of join combinations is large, only include the join paths with one or two joins.

Provide the result in JSON format, where the keys are the number of joins (as integers), and the values are lists of possible paths (each path is a list of table names that can be joined together using that number of joins).

Database Schema:
"""

        # Add tables and their foreign keys to the prompt
        for table_name, info in tables.items():
            prompt += f"Table: {table_name}\n"
            prompt += "Columns:\n"
            for column_name, column_info in info['columns'].items():
                prompt += f"- {column_name} ({column_info['data_type']})\n"
            if info.get('primary_keys'):
                prompt += f"Primary Keys: {', '.join(info['primary_keys'])}\n"
            if info.get('foreign_keys'):
                for fk in info['foreign_keys']:
                    prompt += f"Foreign Key: {fk['column']} references {fk['references']['table']}({fk['references']['column']})\n"
            prompt += "\n"

        prompt += """
Please only return the JSON result with the following structure:
{
    "1": [ ["table1", "table2"], ["table3", "table4"], ... ],
    "2": [ ["table1", "table2", "table3"], ... ],
    ...
}

Where the keys are the number of joins (as integers), and the values are lists of paths (each path is a list of table names that can be joined together using that number of joins).
"""

        self.log(f"LLM Prompt for generating joinable paths:\n{prompt}")

        # Invoke LLM to generate joinable paths
        joinable_paths = self.llm.get_GPT_response_json(prompt, json_format=True)

        if not joinable_paths:
            self.log("LLM failed to generate joinable paths.")
            return {}

        # Save joinable paths to JSON file
        joinable_path_path = self.joinable_path_path
        os.makedirs(os.path.dirname(joinable_path_path), exist_ok=True)
        with open(joinable_path_path, 'w') as f:
            json.dump(joinable_paths, f, indent=4)

        self.log(f"Joinable paths saved to {joinable_path_path}. Joinable paths:\n{json.dumps(joinable_paths, indent=4)}")
        return joinable_paths

    def generate_prompts(self, target_real_constraint, semantic_requirements=None):
        """
        Generate prompts for LLM based on constraints.
        """
        self.log("Starting constrained prompt generation.")

        # Load the template constraints
        constraint_path = f"{self.constraint_path}/{target_real_constraint}"
        with open(constraint_path, 'r') as f:
            constraints_file = json.load(f)

        templates = constraints_file['templates']

        # Assign semantic requirements proportionally
        assigned_templates = []
        if semantic_requirements:
            total_requirements = sum(num for num, _ in semantic_requirements)
            total_templates = len(templates)
            assigned_counts = []
            total_assigned = 0

            # Calculate the initial counts based on proportions
            for num, _ in semantic_requirements:
                count = int(num * total_templates / total_requirements)
                assigned_counts.append(count)
                total_assigned += count

            # Adjust for rounding errors
            i = 0
            while total_assigned < total_templates:
                assigned_counts[i % len(assigned_counts)] += 1
                total_assigned += 1
                i += 1

            i = 0
            while total_assigned > total_templates:
                if assigned_counts[i % len(assigned_counts)] > 0:
                    assigned_counts[i % len(assigned_counts)] -= 1
                    total_assigned -= 1
                i += 1

            # Build the list of assigned templates
            for count, (_, req) in zip(assigned_counts, semantic_requirements):
                assigned_templates.extend([req] * count)

            random.shuffle(assigned_templates)
        else:
            assigned_templates = [None] * len(templates)

        prompts = []
        for template_info, semantic_requirement in zip(templates, assigned_templates):
            num_joins = template_info['num_joins']
            num_aggregations = template_info['num_aggregations']
            num_tables_accessed = len(template_info['read_table_ids'])

            self_join = True
            if int(num_tables_accessed) == (num_joins + 1):
                self_join = False

            # transform the numbers from the constrained database based on the number of tables in the target database
            num_tables_target = len(self.db_schema['tables'])
            num_tables_constraint = int(constraints_file["num_tables"])

            num_joins = math.ceil(num_joins / num_tables_constraint * num_tables_target)
            num_aggregations = math.ceil(num_aggregations / num_tables_constraint * num_tables_target)
            num_tables_accessed = math.ceil(num_tables_accessed / num_tables_constraint * num_tables_target)

            # handle self-join situation
            if not self_join:
                max_num_tables = max(num_tables_accessed, num_joins + 1)
                num_tables_accessed = max_num_tables
                num_joins = num_tables_accessed - 1

            # Select a joinable path based on num_tables_accessed and num_joins
            if num_joins != 0:
                possible_paths = self.joinable_paths.get(str(num_joins), [])
                if not possible_paths:
                    self.log(f"No joinable paths found for num_joins = {num_joins}, providing all tables information.")
                    tables_info = self.db_schema['tables']
                else:
                    joinable_path = random.choice(possible_paths)

                    # Get table schemas for the selected path
                    tables_info = {table: self.db_schema['tables'][table] for table in joinable_path}
            else:
                # Randomly select a table and include it in tables_info
                random_table = random.choice(list(self.db_schema['tables'].keys()))
                tables_info = {random_table: self.db_schema['tables'][random_table]}

            # Build the constraints
            constraints = {
                'num_tables_accessed': num_tables_accessed,  # Calculated based on unique tables
                'num_joins': num_joins,
                'num_aggregations': num_aggregations,
                'semantic_requirement': semantic_requirement,
                'tables_involved': list(tables_info.keys())
            }

            # Build the prompt
            prompt = self.build_prompt(constraints, tables_info)

            # Append to prompts list and templates_info
            prompts.append({'template_id': template_info['template_id'], 'prompt': prompt, 'constraints': constraints})
            self.templates_info.append({'template_id': template_info['template_id'], 'constraints': constraints})

        return prompts

    def build_prompt(self, constraints, tables_info):
        """
        Build the prompt for LLM.
        """
        # Create a prompt template
        prompt = f"""
Generate an SQL template with placeholders for predicate values that satisfies the following constraints:
- Number of unique tables accessed: {constraints['num_tables_accessed']}
- Number of joins: {constraints['num_joins']}
- Number of aggregations: {constraints['num_aggregations']}
"""

        if constraints['semantic_requirement']:
            prompt += f"- Semantic Requirement: {constraints['semantic_requirement']}\n"

        prompt += "Use the following table schemas. Use the following table schemas. Only the exact table and column names provided in these schemas are allowed. Any other column name is not allowed.\n"
        prompt += f"{json.dumps(tables_info, indent=4)}"
        prompt += "\n"

        prompt += """
Format Requirement:
- Predicate values (the dynamic values that will be inserted for filtering) should be wrapped in double curly braces with single quotes like `'{{{{}}}}'`.
- Ensure that all predicate values wrapped in double curly braces are enclosed in single quotes, e.g., `'{{{{real_table_name.real_column_name}}}}'`.
- Table names, column names, and JOIN conditions should be written directly without any curly braces or quotes. Double curly braces with single quotes are only for placeholders where predicate values will be inserted.
- For predicates with both lower and upper bounds, use `'{{{{real_table_name.real_column_name_start}}}}'` and `'{{{{real_table_name.real_column_name_end}}}}'` to represent the placeholder values, but do not wrap the actual column names in curly braces.
- The table names and column names should exactly match those in the database. Include both real table name and column name like `'{{{{real_table_name.real_column_name}}}}'`.
"""

        prompt += """
Hints:
- If the number of joins exceeds 1 + the number of unique tables accessed, then the query must use self-joins or repeatedly join the same set of tables.
- Do not use predicate values that require aggregation. For example, expressions like real_table_name.real_column_name_min, max, count, sum, or any other aggregation functions are not allowed. Predicate values must be directly accessible from the database and must follow the format real_table_name.real_column_name
- When constructing predicate conditions, do not use string matching at all. This type of condition is currently not supported.
"""

        prompt += """
Now let's think step by step and provide the SQL query template. Return the result in JSON format as:
{
    "sql_template": "Your SQL template here",
    "think_process": "Your step by step thinking here"
}
"""

        return prompt

    def clean_placeholder(self, placeholder_str, db_schema_tables):
        """
        Given a placeholder string like 'orders.o_totalprice_min_max'
        or 'orders.o_custkey_start', remove any disallowed suffixes
        and only keep:
        - tableName.columnName
        - tableName.columnName_start
        - tableName.columnName_end
        
        db_schema_tables is typically self.db_schema['tables']:
            {
            "orders": {
                "columns": {
                "o_totalprice": {...},
                "o_custkey": {...},
                ...
                },
                ...
            },
            ...
            }

        Return the "cleaned" placeholder string (e.g. "orders.o_totalprice")
        or None if invalid (cannot match a column in the DB schema).
        """
        # Example input: "orders.o_totalprice_min_max"
        # We want to end up with "orders.o_totalprice" if "o_totalprice" is valid.

        if '.' not in placeholder_str:
            # Not in the form table.col at all
            return None

        table_name, col_str = placeholder_str.split('.', 1)

        # Ensure the table is known
        if table_name not in db_schema_tables:
            return None

        valid_columns = db_schema_tables[table_name]['columns'].keys()

        # 1) Handle _start and _end exactly
        if col_str.endswith('_start'):
            base_col = col_str[:-6]  # remove "_start"
            if base_col in valid_columns:
                return f"{table_name}.{base_col}_start"
            return None

        if col_str.endswith('_end'):
            base_col = col_str[:-4]  # remove "_end"
            if base_col in valid_columns:
                return f"{table_name}.{base_col}_end"
            return None

        # 2) Otherwise, remove everything after the last underscore until we find a valid column 
        #    (or until there are no underscores left).
        while '_' in col_str and col_str not in valid_columns:
            # Remove the trailing underscore block
            # e.g. "o_totalprice_min_max" -> "o_totalprice_min"
            # and then check again.
            col_str = col_str[:col_str.rfind('_')]

        # Finally, if col_str is in the valid columns, return it
        if col_str in valid_columns:
            return f"{table_name}.{col_str}"

        # If none of the above matched, it's invalid
        return None

    def fix_sql_template_placeholders(self, sql_template, db_schema_tables):
        """
        Find all placeholders of the form '...{{tableName.columnName...}}...'
        Clean them up so that they only have:
            (1) tableName.columnName
            (2) tableName.columnName_start
            (3) tableName.columnName_end

        Return the modified SQL template.
        """
        # Regex to match `'{{tableName.columnName}}'`, capturing the part inside {{...}}.
        # Because your placeholders in the final SQL appear as '...{{table.col}}...', 
        # we search for single quote, then {{, then the inside, then }}, then single quote.
        placeholder_pattern = re.compile(r"'?{{(\w+\.\w+)}}'?")

        def replacer(match):
            # Entire match includes the single quotes and curly braces: e.g. `'{{orders.o_totalprice_min}}'`
            full_match = match.group(0)
            inside = match.group(1)  # e.g. "orders.o_totalprice_min"

            # Use the helper to clean it
            cleaned = self.clean_placeholder(inside, db_schema_tables)
            if cleaned is None:
                # If invalid, you could either remove the placeholder entirely or leave it as is.
                # Let's return it unchanged, or you could return an empty string.
                return full_match

            # Return the corrected placeholder, e.g. `'{{orders.o_totalprice}}'`
            return f"'{{{{{cleaned}}}}}'"

        # Substitutes each placeholder with its cleaned version
        fixed_template = placeholder_pattern.sub(replacer, sql_template)
        return fixed_template

    def generate_sql_templates(self, prompts):
        """
        Generate SQL templates using LLM and store them.
        """
        self.log("Starting SQL template generation.")

        folder_path = self.folder_path
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        # invoke GPT in parallel
        template_id_list = []
        prompt_list = []
        constraint_list = []
        for i, template_data in enumerate(prompts):
            template_id_list.append(template_data['template_id'])
            prompt_list.append(template_data['prompt'])
            constraint_list.append(template_data['constraints'])
        
        self.log(prompt_list)

        sql_template_list = self.llm.invoke_GPT_in_parallel(prompt_list)
        sql_template_list = [template.get('sql_template', '') for template in sql_template_list]
        self.log(sql_template_list)

        for i in range(len(template_id_list)):
            template_id = template_id_list[i]
            constraints = constraint_list[i]

            raw_sql_template = sql_template_list[i]
            sql_template = self.fix_sql_template_placeholders(raw_sql_template, self.db_schema["tables"])

            # Save the SQL template
            file_name = f"template_{template_id}.sql"
            file_path = os.path.join(folder_path, file_name)

            # Add meta information as comments
            creation_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            meta_info = (
                f"-- SQL Template Metadata\n"
                f"-- Template ID: {template_id}\n"
                f"-- Creation Time: {creation_time}\n"
                f"-- LLM Model: {self.llm.model}\n"
                f"-- Constraints:\n"
                f"--   Number of unique Tables Accessed: {constraints['num_tables_accessed']}\n"
                f"--   Number of Joins: {constraints['num_joins']}\n"
                f"--   Number of Aggregations: {constraints['num_aggregations']}\n"
                f"--   Semantic Requirement: {constraints['semantic_requirement']}\n"
                f"--   Tables Involved: {constraints['tables_involved']}\n"
                f"\n"
            )

            formatted_sql = sqlparse.format(sql_template, reindent=True, keyword_case="upper")
            self.log(f"Generated SQL Template:\n{formatted_sql}")

            with open(file_path, 'w') as f:
                f.write(meta_info + formatted_sql)

        # Save templates_info to a JSON file
        templates_info_path = os.path.join(self.folder_path, "templates_info.json")
        with open(templates_info_path, 'w') as f:
            json.dump(self.templates_info, f, indent=4)

        self.log(f"Templates info saved to {templates_info_path}")

    def get_sample_values(self, sql_template):
        # Extract placeholders from the SQL template
        placeholders = re.findall(r"'{{(.*?)}}'", sql_template)
        placeholders += re.findall(r"{{(.*?)}}", sql_template)
        placeholders = set(placeholders)  # Remove duplicates

        sample_values = {}
        for placeholder in placeholders:
            # Handle placeholders like 'table.column' or 'table.column_start'
            if '.' in placeholder:
                parts = placeholder.split('.')
                table = parts[0]
                column = parts[1]
                # Remove '_start' or '_end' if present
                column = re.sub(r'_(start|end)$', '', column)
                key = f"{table}.{column}"
                if key not in sample_values:
                    # Fetch a sample value
                    query = f"SELECT {column} FROM {table} WHERE {column} IS NOT NULL LIMIT 1;"
                    result = self.db_controller.execute_sql(query)
                    if result.get('error') is None and result.get('result'):
                        value = result['result'][0][0]
                        sample_values[key] = value
                    else:
                        # If unable to fetch, use default values
                        sample_values[key] = "'test'"  # Default to string
            else:
                # Placeholder doesn't match expected format
                sample_values[placeholder] = "'test'"

        return sample_values

    def identify_placeholders(self, sql_template):
        """ 
            Extract placeholders from the SQL template 
        """
        placeholder_pattern = r"\{\{(\w+\.\w+)\}\}"
        placeholders = re.findall(placeholder_pattern, sql_template)

        return placeholders

    def replace_placeholders(self, sql_template, sample_values):
        # Replace placeholders with actual values
        sql = sql_template

        placeholders = self.identify_placeholders(sql)

        for placeholder in placeholders:
            sub_placeholder = re.sub(r'_(start|end)$', '', placeholder)

            value = sample_values.get(sub_placeholder, "'test'")
            if isinstance(value, str):
                value = value.strip()
            else:
                value = str(value)

            sql = sql.replace(f"{{{{{placeholder}}}}}", value)

        return sql

    def check_and_rewrite_templates_parallel(self):
        """
        Check and possibly rewrite generated SQL templates in parallel.
        """
        self.log("Starting SQL template check and rewrite.")

        # Load templates_info
        templates_info_path = os.path.join(self.folder_path, "templates_info.json")
        with open(templates_info_path, 'r') as f:
            templates_info = json.load(f)

        # Build a mapping from template_id to constraints
        template_constraints = {str(info['template_id']): info['constraints'] for info in templates_info}

        folder_path = self.folder_path
        template_files = [
            f for f in os.listdir(folder_path) 
            if f.startswith('template_') and f.endswith('.sql')
        ]

        # Helper function to process a single file
        def process_single_file(file_name):
            file_path = os.path.join(folder_path, file_name)
            with open(file_path, 'r') as f:
                sql_template_content = f.read()

            # Extract template_id from file name
            template_id = file_name.split('_')[1].split('.')[0]

            self.log(f"Processing template {file_name}")

            # Initialize retry counters
            max_constraints_retries = 5
            max_grammar_check_retries = 5

            # Extract meta information and SQL query
            meta_lines = []
            sql_query_lines = []
            in_meta = True
            for line in sql_template_content.split('\n'):
                if in_meta and line.startswith('--'):
                    meta_lines.append(line)
                else:
                    in_meta = False
                    sql_query_lines.append(line)
            meta_info = '\n'.join(meta_lines)
            sql_query = '\n'.join(sql_query_lines).strip()

            # Extract initial Rewrite Attempts Numbers if they exist
            constraints_rewrite_prefix = '-- Rewrite Attempts Number for Constraints Check:'
            grammar_rewrite_prefix = '-- Rewrite Attempts Number for Grammar Check:'
            constraints_retries = 0
            grammar_check_retries = 0
            new_meta_lines = []
            for line in meta_lines:
                if line.startswith(constraints_rewrite_prefix):
                    constraints_retries = int(line[len(constraints_rewrite_prefix):].strip())
                    # Don't keep the old rewrite line — we will add the updated one later
                elif line.startswith(grammar_rewrite_prefix):
                    grammar_check_retries = int(line[len(grammar_rewrite_prefix):].strip())
                    # Don't keep the old rewrite line — we will add the updated one later
                else:
                    new_meta_lines.append(line)

            constraints = template_constraints.get(template_id, {})

            # Constraints checking loop
            while constraints_retries < max_constraints_retries:
                # Update meta information with the current retry attempts
                updated_meta_lines = new_meta_lines.copy()
                updated_meta_lines.append(f'{constraints_rewrite_prefix} {constraints_retries}')
                updated_meta_lines.append(f'{grammar_rewrite_prefix} {grammar_check_retries}')
                updated_meta_info = '\n'.join(updated_meta_lines)

                # Combine updated meta information with the SQL query
                current_sql_template = f'{updated_meta_info}\n{sql_query}'

                # Prepare the prompt
                prompt = f"""
    Given the following SQL query template and the associated constraints:

    SQL Template and Constraints:
    {current_sql_template}

    Other constraints:
    - If the number of joins is larger than 1 + the number of unique table accessed, use self joins or join the same set of tables repeatedly 
    - Do not use predicate values that require aggregation. For example, expressions like real_table_name.real_column_name_min, max, count, sum, or any other aggregation functions are not allowed. Predicate values must be directly accessible from the database and must follow the format real_table_name.real_column_name

    Thinks step by step and check if the SQL template satisfies all the constraints.

    If it satisfies all the constraints, respond in JSON format:
    {{
        "result": "Satisfied",
        "think_process": "Your step by step thinking here"
    }}

    If not, let's think step by step and provide the reasons why it does not satisfy the constraints, how to modify it, and the corrected SQL template. 
    Ensure the corrected SQL template includes the meta information. Don't update the attempt number. 

    Format Requirement for SQL template:
    - Predicate values (the dynamic values that will be inserted for filtering) should be wrapped in double curly braces with single quotes like `'{{{{}}}}'`.
    - Ensure that all predicate values wrapped in double curly braces are enclosed in single quotes, e.g., `'{{{{real_table_name.real_column_name}}}}'`.
    - Table names, column names, and JOIN conditions should be written directly without any curly braces or quotes. Double curly braces with single quotes are only for placeholders where predicate values will be inserted.
    - For predicates with both lower and upper bounds, use `'{{{{real_table_name.real_column_name_start}}}}'` and `'{{{{real_table_name.real_column_name_end}}}}'` to represent the placeholder values, but do not wrap the actual column names in curly braces.
    - The table names and column names should exactly match those in the database. Include both real table name and column name like `'{{{{real_table_name.real_column_name}}}}'`.

    Respond in JSON format:
    {{
        "result": "Not Satisfied/Satisfied",
        "reason": "Your step by step thinking and reason here",
        "modification": "How to modify it",
        "sql_template": "Your corrected SQL template here, including the meta information"
    }}
    """
                # Call LLM
                response = self.llm.get_GPT_response_json(prompt, json_format=True)

                if response.get("result") == "Satisfied":
                    self.log(f"Template {file_name} satisfies the constraints.")
                    # Save the template with updated meta information
                    formatted_sql = sqlparse.format(current_sql_template, reindent=True, keyword_case="upper")
                    with open(file_path, 'w') as f:
                        f.write(formatted_sql)

                    # Proceed to grammar checking
                    while grammar_check_retries < max_grammar_check_retries:
                        # Update meta information with the current retry attempts
                        updated_meta_lines = new_meta_lines.copy()
                        updated_meta_lines.append(f'{constraints_rewrite_prefix} {constraints_retries}')
                        updated_meta_lines.append(f'{grammar_rewrite_prefix} {grammar_check_retries}')
                        updated_meta_info = '\n'.join(updated_meta_lines)
                        current_sql_template = f'{updated_meta_info}\n{sql_query}'

                        # Prepare the SQL for execution by replacing placeholders with real values
                        sample_values = self.get_sample_values(sql_query)
                        executable_sql_query = self.replace_placeholders(sql_query, sample_values)
                        explain_sql = f"EXPLAIN {executable_sql_query}"

                        # Execute the EXPLAIN SQL on the DBMS
                        execution_result = self.db_controller.execute_sql(explain_sql)

                        if execution_result.get('error') is None:
                            self.log(f"Template {file_name} passed the grammar check.")
                            # Save the template with updated meta information
                            formatted_sql = sqlparse.format(current_sql_template, reindent=True, keyword_case="upper")
                            with open(file_path, 'w') as f:
                                f.write(formatted_sql)
                            break  # proceed to next file
                        else:
                            # There is an error
                            error_message = execution_result.get('error')
                            self.log(f"Grammar check error for template {file_name}: {error_message}")

                            # Prepare prompt for LLM
                            prompt = f"""
    Given the following SQL template and the error message from the DBMS:

    SQL Template:
    {current_sql_template}

    Error Message:
    {error_message}

    Two Common Errors:
    1. Check whether there are predicates that require aggregation. If so, modify it. Expressions like real_table_name.real_column_name_min, max, count, sum, or any other aggregation functions are not allowed. Predicate values must be directly accessible from the database
    2. Check whether the predicates really refer to columns in the corresponding table. Every predicate value should come from one column.
    This is the columns in the table used by the SQL template. You can use this to know whether the predicate/column exists in the table.
    {self.collect_table_columns(current_sql_template, self.db_schema)}

    Please fix the SQL template to correct the error, ensuring that it satisfies all the constraints and follows the format requirements.
    Ensure the corrected SQL template includes the meta information. Do not update the rewrite attempt number.
    
    Format Requirement for SQL template:
    - Predicate values (the dynamic values that will be inserted for filtering) should be wrapped in double curly braces with single quotes like `'{{{{}}}}'`.
    - Ensure that all predicate values wrapped in double curly braces are enclosed in single quotes, e.g., `'{{{{real_table_name.real_column_name}}}}'`.
    - Table names, column names, and JOIN conditions should be written directly without any curly braces or quotes. Double curly braces with single quotes are only for placeholders where predicate values will be inserted.
    - For predicates with both lower and upper bounds, use `'{{{{real_table_name.real_column_name_start}}}}'` and `'{{{{real_table_name.real_column_name_end}}}}'` to represent the placeholder values, but do not wrap the actual column names in curly braces.
    - The table names and column names should exactly match those in the database. Include both real table name and column name like `'{{{{real_table_name.real_column_name}}}}'`.

    Note:
    - If you see 'test' in the SQL templates, it means no predicate value can be obtained from database. Possibly the column does not exist in database, you should use the correct column name, or the column really exist in the corresponding table.

    Now let's think step by step and respond in JSON format:
    {{
        "think_process": "Your step by step thinking here",
        "sql_template": "Your corrected SQL template here, including the meta information"
    }}
    """
                            response = self.llm.get_GPT_response_json(prompt, json_format=True)
                            corrected_sql_template = response.get('sql_template')

                            if corrected_sql_template:
                                # Update the sql_query and meta_info for the next iteration
                                meta_lines = []
                                sql_query_lines = []
                                in_meta = True
                                for line in corrected_sql_template.split('\n'):
                                    if in_meta and line.startswith('--'):
                                        meta_lines.append(line)
                                    else:
                                        in_meta = False
                                        sql_query_lines.append(line)
                                meta_info = '\n'.join(meta_lines)
                                sql_query = '\n'.join(sql_query_lines).strip()

                                # Update new_meta_lines with the meta lines excluding the rewrite attempts
                                new_meta_lines = [
                                    line for line in meta_lines 
                                    if not line.startswith(constraints_rewrite_prefix) 
                                    and not line.startswith(grammar_rewrite_prefix)
                                ]

                                # Save the corrected template
                                formatted_sql = sqlparse.format(corrected_sql_template, reindent=True, keyword_case="upper")
                                with open(file_path, 'w') as f:
                                    f.write(formatted_sql)

                                grammar_check_retries += 1
                            else:
                                self.log(f"LLM failed to provide a corrected SQL template for template ID {template_id}")
                                break
                    else:
                        self.log(f"Template {file_name} did not pass the grammar check "
                                f"after {max_grammar_check_retries} retries.")
                    # Once constraints are satisfied and grammar check loop is done, exit constraints loop
                    break
                else:
                    constraints_retries += 1

                    # The template does not satisfy the constraints
                    reason = response.get("reason", "No reason provided.")
                    modification = response.get("modification", "No modification provided.")
                    new_sql_template = response.get("sql_template", '')

                    self.log(f"Attempt {constraints_retries}: Template {file_name} does not satisfy the constraints.")
                    self.log(f"Reason: {reason}")
                    self.log(f"Modification: {modification}")
                    self.log(f"Rewritten SQL Template:\n{new_sql_template}")

                    if not new_sql_template:
                        self.log(f"LLM failed to provide a rewritten SQL template for template ID {template_id}")
                        break

                    # Update the sql_query and meta_info for the next iteration
                    meta_lines = []
                    sql_query_lines = []
                    in_meta = True
                    for line in new_sql_template.split('\n'):
                        if in_meta and line.startswith('--'):
                            meta_lines.append(line)
                        else:
                            in_meta = False
                            sql_query_lines.append(line)
                    meta_info = '\n'.join(meta_lines)
                    sql_query = '\n'.join(sql_query_lines).strip()

                    # Update new_meta_lines with the meta lines excluding the rewrite attempts
                    new_meta_lines = [
                        line for line in meta_lines 
                        # if not line.startswith(constraints_rewrite_prefix) 
                        # and not line.startswith(grammar_rewrite_prefix)
                    ]

                    # Save the new template
                    formatted_sql = sqlparse.format(new_sql_template, reindent=True, keyword_case="upper")
                    with open(file_path, 'w') as f:
                        f.write(formatted_sql)
            else:
                self.log(f"Template {file_name} did not satisfy the constraints "
                        f"after {max_constraints_retries} retries.")

        # Use a thread pool to process files in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_single_file, file_name) for file_name in template_files]
            # Optionally, collect results or exceptions:
            for future in concurrent.futures.as_completed(futures):
                # If there's an exception, it will be raised here
                future.result()

        self.log("Finished SQL template check and rewrite in parallel.")

    def collect_table_columns(self, current_sql_template: str, db_schema: dict) -> dict[str, list[str]]:
        """
        {table_name: [all its columns]} for every table referenced after FROM/JOIN
        (case-insensitive) that also exists in `db_schema`.
        """
        # 1. regex to capture the first token after FROM / JOIN
        tbl_regex = re.compile(r'\b(?:FROM|JOIN)\s+([A-Za-z0-9_."`]+)', re.IGNORECASE)

        # 2. gather table names found in the SQL template
        tables_found = set()
        for m in tbl_regex.finditer(current_sql_template):
            raw = m.group(1).rstrip(',').strip('"`')          # remove trailing comma & quotes
            table = raw.split('.')[-1]                        # drop any schema prefix
            tables_found.add(table.lower())                   # normalize to lower-case

        # 3. build a lower-case lookup → canonical name from the schema
        schema_tables = db_schema.get("tables", {})
        canon_lookup = {name.lower(): name for name in schema_tables}

        # 4. return every matched table with *all* its columns
        return {
            canon_lookup[tbl_lc]: sorted(schema_tables[canon_lookup[tbl_lc]]["columns"].keys())
            for tbl_lc in tables_found
            if tbl_lc in canon_lookup            # ignore names absent from the schema
        }

    def refine_templates(self, cost_type, old_sql_templates, old_costs_list, target_cost_range):
        """
        Attempt to refine SQL templates so that their resulting query costs 
        shift into the given target_cost_range. 
        
        This enhanced version supports few-shot learning with multiple templates.
        
        Args:
            cost_type (str): The type of cost metric (cost, time, cardinality).
            old_sql_templates (str or list): The original SQL template string(s).
            old_costs_list (list or list of lists): The costs observed for template(s).
            target_cost_range (tuple or list): [min_cost, max_cost] that we want templates 
                                            to aim for in future executions.

        Returns:
            list of str: A list of newly refined SQL templates proposed by the LLM.
        """
        self.log("Starting template refinement with few-shot learning.")
        
        # Handle single template case
        if isinstance(old_sql_templates, str):
            old_sql_templates = [old_sql_templates]
            old_costs_list = [old_costs_list]
        
        # Ensure we have at most 3 templates for few-shot learning
        if len(old_sql_templates) > 3:
            # Keep only the 3 closest templates to the target range
            template_distances = []
            for i, costs in enumerate(old_costs_list):
                if not costs:
                    continue
                # Calculate how close each template's average cost is to the target range
                valid_costs = [c for c in costs if c is not None]
                if not valid_costs:
                    continue
                    
                avg_cost = sum(valid_costs) / len(valid_costs)
                min_target, max_target = target_cost_range
                # Distance to target range
                if avg_cost < min_target:
                    distance = min_target - avg_cost
                elif avg_cost > max_target:
                    distance = avg_cost - max_target
                else:
                    distance = 0  # Already in range
                template_distances.append((i, distance))
            
            # Sort by distance (ascending)
            template_distances.sort(key=lambda x: x[1])
            
            # Keep only the 3 closest templates
            indices = [i for i, _ in template_distances[:3]]
            old_sql_templates = [old_sql_templates[i] for i in indices]
            old_costs_list = [old_costs_list[i] for i in indices]
        
        # Gather additional data for all templates
        template_analysis = []
        for i, (template, costs) in enumerate(zip(old_sql_templates, old_costs_list)):
            # Filter out None values
            costs = [c for c in costs if c is not None]
            if not costs:
                continue
                
            avg_cost = sum(costs) / len(costs) if costs else 0.0
            min_cost = min(costs) if costs else 0.0
            max_cost = max(costs) if costs else 0.0
            distinct_cost = len(set(costs))
            num_joins = self.parse_number_of_joins(template)
            
            template_analysis.append({
                "template": template,
                "avg_cost": avg_cost,
                "min_cost": min_cost,
                "max_cost": max_cost,
                "distinct_cost": distinct_cost,
                "num_joins": num_joins,
                "num_costs": len(costs)
            })
        
        if not template_analysis:
            self.log("No valid templates with costs for refinement.")
            return []
        
        # Get overall schema information for all templates
        all_num_joins = set(analysis["num_joins"] for analysis in template_analysis)
        possible_tables = set()
        
        for num_joins in all_num_joins:
            possible_paths = self.joinable_paths.get(str(num_joins), [])
            for path in possible_paths:
                possible_tables.update(path)
        
        # If no tables were found from join paths (which happens when there are no joins),
        # include all tables from the database schema instead of having an empty result
        if not possible_tables:
            possible_tables = set(self.db_schema["tables"].keys())

        possible_tables = list(possible_tables)
        
        filtered_schema = {
            table: {
                "size": self.db_schema["tables"][table]["size"],
                "row_count": self.db_schema["tables"][table]["row_count"],
                "columns": {
                    col: self.db_schema["tables"][table]["columns"][col]["unique_values"]
                    for col in self.db_schema["tables"][table]["columns"]
                },
            }
            for table in possible_tables
        }
        
        # Format the cost type name for the prompt
        if cost_type == "cost":
            cost_type_name = "execution plan cost"
        elif cost_type == "time":
            cost_type_name = "execution time"
        elif cost_type == "cardinality":
            cost_type_name = "sum of all the cardinalities in the execution plan"
        else:
            cost_type_name = cost_type
        
        # Construct the prompt with few-shot examples
        template_examples = ""
        for i, analysis in enumerate(template_analysis):
            possible_paths = self.joinable_paths.get(str(analysis["num_joins"]), [])
            
            template_examples += f"""
    Example Template {i+1}:
    SQL Template: {analysis["template"]}
    Historical Cost Range: [{analysis["min_cost"]}, {analysis["max_cost"]}]
    Average Cost: {analysis["avg_cost"]:.2f}
    Distinct Cost Values: {analysis["distinct_cost"]} from {analysis["num_costs"]} costs
    Number of JOINs: {analysis["num_joins"]}
    Possible JOIN paths for {analysis["num_joins"]} joins:
    {json.dumps(possible_paths, indent=4)}

    """
        
        min_target_cost, max_target_cost = target_cost_range
        
        prompt = f"""
    We want to generate SQL queries with certain cost type: {cost_type_name}.

    You are given:
    1) {len(template_analysis)} existing SQL templates, where by changing the predicate values, they have historically produced costs in different ranges.
    2) We want to refine or rewrite these templates so that future queries generated using various predicate values 
    will run with a cost in the target range of [{min_target_cost}, {max_target_cost}].

    Here are the existing templates and their cost characteristics:
    {template_examples}

    Table schema information:
    {json.dumps(filtered_schema, indent=4)}

    We have three possible refinement operations:
    (1) Change the accessed table or JOIN path: 
    - If only one table is accessed, we can choose a different table which is larger or smaller
    - If more than one table is accessed
        - Possibly choose different tables or a different order of joins
        - We can adjust the number of joins up or down based on the target cost range
        - Use the provided possible joinable paths based on our database schema

    (2) Change the SQL structure:
    - Make the SQL template more or less complex
    - Add or delete predicate conditions
    - Change the columns used for filters or predicate conditions based on columns selectivity (i.e., the unique values in a column, provided above)

    (3) If it is hard to modify the existing templates to satisfy the target costs, we really encourage you to:
        - Create brand-new SQL templates

    Learn from the examples to understand:
    - Which templates produce costs closest to our target range
    - What patterns lead to higher or lower costs 
    - How join complexity impacts the cost

    We do NOT want to break the basic placeholders format, but you can add, remove, or rename placeholders 
    to shift the cost up/down. For instance, applying more selective predicates might decrease cost, 
    while removing some or joining larger tables might increase cost.

    We want you to:
    - Decide which operation(s) to use (only join path, only structure, or both, or create brand-new SQL templates).
    - Produce a refined SQL template that can push the cost into the target range.
    - Provide metadata explaining what was changed:
    * operation: 'join_path', 'structure', 'both', or 'brand-new'
    * old_join_path -> new_join_path (if changed)
    * table sizes relevant to the changes
    * any relevant new/modified predicates or structural changes

    Finally, respond in **JSON** format as:
    {{
    "sql_template": "Your refined SQL here, note the meta information about sql should be retained",
    "metadata": {{
        "operation": "join_path" or "structure" or "both" or "brand-new",
        "old_join_path": "old join path, display the accessed table name if there is no join",
        "new_join_path": "new join path",
        "table_size_changes": "Describe how you used bigger/smaller tables (if any)",
        "structural_changes": "Describe any structural changes: new filters, group-by, columns selected, predicate conditions",
        "think_process": "A brief reasoning on how you achieved cost shift"
    }}
    }}

    Important notes:
    - Keep using double curly braces with single quotes for placeholders, e.g. `'{{some_table.some_column}}'`.
    - Make sure don't use constant value as predicate value since you don't know which values are available for that column in database.
    - If you do not change the path, set "new_join_path" equal to "same as old".
    - If you do not change the structure, set "structural_changes" to "none".
    - Make sure the refined SQL is valid enough to parse.
    - The refined SQL template should still satisfy the constraints listed in the old SQL template

    Now let's think step by step. Return your answer in valid JSON.
    """
        self.log(f"LLM prompt for refinement with few-shot learning:\n{prompt}")

        # Call LLM to get the refined template
        response_json = self.llm.get_GPT_response_json(prompt, json_format=True)
        if not response_json:
            self.log("LLM returned no response for refine_templates.")
            return []

        # Process the LLM response
        refined_templates = []
        if isinstance(response_json, dict):
            # Single response
            sql_template = response_json.get("sql_template", "").strip()
            metadata = response_json.get("metadata", {})
            if sql_template:
                new_template = self._inject_refinement_metadata(sql_template, metadata)
                refined_templates.append(new_template)
        elif isinstance(response_json, list):
            # Possibly multiple responses
            for item in response_json:
                if not isinstance(item, dict):
                    continue
                sql_template = item.get("sql_template", "").strip()
                metadata = item.get("metadata", {})
                if sql_template:
                    new_template = self._inject_refinement_metadata(sql_template, metadata)
                    refined_templates.append(new_template)

        if not refined_templates:
            self.log("LLM did not provide any refined SQL template in expected JSON format.")

        return refined_templates

    def parse_number_of_joins(self, old_sql_template):
        """
        Look through the comment lines in old_sql_template to find the line containing
        'Number of Joins: X', and return X as an integer. 
        If not found, count occurrences of JOIN in the template.
        If no JOINs found, return None.
        """
        # First approach: Look for metadata comment
        lines = old_sql_template.split('\n')
        
        for line in lines:
            # We stop if we encounter a non-comment line (the actual SQL), so we only scan metadata.
            if not line.strip().startswith('--'):
                continue
                
            # Example line: "--   Number of Joins: 0"
            if 'Number of Joins:' in line:
                # Remove the leading '--' and extra spaces
                content = line.lstrip('-').strip()
                # content might be: "Number of Joins: 0"
                parts = content.split(':', maxsplit=1)
                if len(parts) == 2 and 'Number of Joins' in parts[0]:
                    joins_str = parts[1].strip()
                    try:
                        return int(joins_str)
                    except ValueError:
                        pass
        
        # Second approach: Count occurrences of JOIN in the template
        join_count = old_sql_template.upper().count('JOIN')
        if join_count > 0:
            return join_count
        
        # If both approaches fail, return None
        return None

    def _inject_refinement_metadata(self, new_sql, metadata):
        """
        Insert metadata as SQL comments at the top of the refined SQL template, 
        so it can be tracked later.

        metadata might look like:
        {
        "operation": "join_path" or "structure" or "both",
        "old_join_path": "...",
        "new_join_path": "...",
        "table_size_changes": "...",
        "structural_changes": "...",
        "think_process": "..."
        }
        """
        timestamp_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        meta_comment_lines = [
            "-- Refined SQL Template Metadata",
            f"-- Refinement Time: {timestamp_str}",
            f"-- Operation: {metadata.get('operation', 'unknown')}",
            f"-- Old Join Path: {metadata.get('old_join_path', 'unknown')}",
            f"-- New Join Path: {metadata.get('new_join_path', 'unknown')}",
            f"-- Table Size Changes: {metadata.get('table_size_changes', 'N/A')}",
            f"-- Structural Changes: {metadata.get('structural_changes', 'N/A')}",
            f"-- LLM Reasoning: {metadata.get('think_process', 'N/A')}"
        ]
        
        meta_info_block = "\n".join(meta_comment_lines) + "\n" + "\n"

        # Format the final refined template with upper-case keywords if desired
        formatted_new_sql = sqlparse.format(new_sql, reindent=True, keyword_case="upper")

        return meta_info_block + formatted_new_sql
       
    def load_sql_templates(self):
        """
        Load all template_i.sql files from the given folder into a list.
        """
        folder_path = self.folder_path

        template_ids = []
        templates = []
        # Iterate over all files in the folder that match template_i.sql pattern
        for filename in os.listdir(folder_path):
            if filename.startswith('template_') and filename.endswith('.sql'):
                file_path = os.path.join(folder_path, filename)

                # Read the content of each template file
                with open(file_path, 'r') as file:
                    sql_template = file.read()
                    template_ids.append(f"{filename.split('.')[0]}")
                    templates.append(sql_template)

        return template_ids, templates