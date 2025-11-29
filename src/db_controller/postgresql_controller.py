from db_controller.base_controller import BaseDBController
import psycopg2
import time, os
import json, decimal, datetime

class PostgreSQLController(BaseDBController):
    """ Instantiate DBMSTemplate to support PostgreSQL DBMS """
    def __init__(self, db, user, password, restart_cmd, recover_script, port):
        super().__init__(db, user, password, restart_cmd, recover_script, port)
        self.name = "postgres"
    
    def _connect(self, db=None):
        """ Establish connection to database, return success flag """
        self.failed_times = 0
        if db==None:
            db=self.db
        print(f'Trying to connect to {db} with user {self.user}')
        while True:
            try:            
                self.connection = psycopg2.connect(
                    database = db, user = self.user, 
                    password = self.password, host = "localhost", port=self.port
                )
                print(f"Success to connect to {db} with user {self.user}")
                return True
            except Exception as e:
                self.failed_times += 1
                print(f'Exception while trying to connect: {e}')
                if self.failed_times >= 4:
                    self.recover_dbms()
                    return False
                print("Reconnet again")
                time.sleep(3)
            
    def _disconnect(self):
        """ Disconnect from database. """
        if self.connection:
            print('Disconnecting ...')
            self.connection.close()
            print('Disconnecting done ...')
            self.connection = None

    def _copy_db(self, target_db, source_db):
        # for tpcc, recover the data for the target db(benchbase)
        self.update_dbms(f'drop database if exists {target_db}')
        print('Dropped old database')
        self.update_dbms(f'create database {target_db} with template {source_db}')
        print('Initialized new database')

    def execute_queries_from_file(self, target_workload):
        
        folder_path = f"./customized_workloads/{target_workload}"
        sql = ""

        if os.path.isdir(folder_path):
            for filename in os.listdir(folder_path):
                # Check if the file is a .sql file
                if filename.endswith(".sql"):
                    # Construct the full path of the .sql file
                    file_path = os.path.join(folder_path, filename)
                    
                    # Open and read the .sql file
                    with open(file_path, 'r') as file:
                        # Read the content of the file and append it to the sql variable
                        sql += file.read() 

            if sql == "":
                return False        
        
            try:
                self.connection.autocommit = True
                cursor = self.connection.cursor()
                sql_statements = sql.split(';')
                for statement in sql_statements:
                    if statement.strip():
                        cursor.execute(statement)
                cursor.close()
                return True
            except Exception as e:
                print(f'Exception execution {sql}: {e}')    
            return False
        else:
            return False
        
    def update_dbms(self, sql):
        """ Execute sql query on dbms to update knob value and return success flag """
        try:
            self.connection.autocommit = True
            cursor = self.connection.cursor()
            cursor.execute(sql)
            cursor.close()
            return True
        except Exception as e:
            print(f"Failed to execute {sql} to update dbms for error: {e}")
            return False 
        
    def execute_sql(self, sql):
        """ Execute SQL on dbms and return the execution result or an error message """
        if self.connection is None:
            if not self._connect():
                print("Failed to reconnect to the database.")
                return {"result": None, "error": "Failed to reconnect to the database"}

        try:
            self.connection.autocommit = True
            cursor = self.connection.cursor()
            cursor.execute(sql)

            try:
                result = cursor.fetchall()  # Try to fetch results
            except psycopg2.ProgrammingError:
                # No results to fetch (e.g., INSERT, UPDATE, DELETE)
                result = None
            
            # Commit changes for non-SELECT queries
            if not sql.strip().lower().startswith("select"):
                self.connection.commit()

            cursor.close()
            return {"result": result, "error": None}  # Return result without an error
        except psycopg2.Error as e:
            # Return the error message
            return {"result": None, "error": str(e)}

    def get_column_info(self, folder_path):
        """
        Fetch all tables and their column metadata including min, max, total distinct count,
        and store up to 500 'sampled_distinct_values' for each column by sampling the actual
        column values in the table. (No random value generation for numeric columns.)

        OPTIMIZED: Uses batched queries to minimize database round-trips.
        """
        
        os.makedirs(folder_path, exist_ok=True)

        table_metadata = {}

        try:
            self.connection.autocommit = True
            cursor = self.connection.cursor()

            # Step 1: Get all table names
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public';
            """)
            tables = cursor.fetchall()

            for table in tables:
                table_name = table[0]
                table_metadata[table_name] = {}

                # Step 2: Get columns for the current table
                cursor.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position;
                """)
                columns = cursor.fetchall()

                # Initialize metadata structure
                for column in columns:
                    column_name, column_type = column
                    table_metadata[table_name][column_name] = {
                        'type': column_type,
                        'min_value': None,
                        'max_value': None,
                        'distinct_count': 0,
                        'sampled_distinct_values': []
                    }

                # Step 3: OPTIMIZED - Batch all min/max/distinct_count into ONE query per table
                if columns:
                    # Build a single query that gets min, max, distinct count for all columns at once
                    select_parts = []
                    for column_name, _ in columns:
                        select_parts.append(f"MIN({column_name}) AS min_{column_name}")
                        select_parts.append(f"MAX({column_name}) AS max_{column_name}")
                        select_parts.append(f"COUNT(DISTINCT {column_name}) AS distinct_{column_name}")

                    combined_query = f"SELECT {', '.join(select_parts)} FROM {table_name};"

                    try:
                        cursor.execute(combined_query)
                        result = cursor.fetchone()

                        # Parse results - every 3 values correspond to one column (min, max, distinct_count)
                        for idx, (column_name, _) in enumerate(columns):
                            col_info = table_metadata[table_name][column_name]
                            col_info['min_value'] = result[idx * 3]
                            col_info['max_value'] = result[idx * 3 + 1]
                            col_info['distinct_count'] = result[idx * 3 + 2]
                    except Exception as e:
                        print(f"Error fetching batch metadata for table {table_name}: {e}")
                        # Fallback to individual queries if batch query fails
                        for column_name, _ in columns:
                            try:
                                cursor.execute(f"""
                                    SELECT MIN({column_name}), MAX({column_name}), COUNT(DISTINCT {column_name})
                                    FROM {table_name};
                                """)
                                min_value, max_value, distinct_count = cursor.fetchone()
                                col_info = table_metadata[table_name][column_name]
                                col_info['min_value'] = min_value
                                col_info['max_value'] = max_value
                                col_info['distinct_count'] = distinct_count
                            except Exception as e2:
                                print(f"Error fetching metadata for column {column_name}: {e2}")

                # Step 4: Retrieve distinct values for each column
                for column_name, _ in columns:
                    try:
                        col_info = table_metadata[table_name][column_name]
                        distinct_count = col_info['distinct_count']

                        if distinct_count <= 500:
                            # Get all distinct values
                            cursor.execute(f"""
                                SELECT DISTINCT {column_name}
                                FROM {table_name}
                                WHERE {column_name} IS NOT NULL
                            """)
                            distinct_vals = [row[0] for row in cursor.fetchall()]
                            col_info['sampled_distinct_values'] = distinct_vals
                        else:
                            # More than 500 distinct values: pick 500 using TABLESAMPLE or LIMIT
                            cursor.execute(f"""
                                SELECT DISTINCT {column_name}
                                FROM {table_name}
                                WHERE {column_name} IS NOT NULL
                                LIMIT 500
                            """)
                            distinct_vals = [row[0] for row in cursor.fetchall()]
                            col_info['sampled_distinct_values'] = distinct_vals
                    except Exception as e:
                        print(f"Error fetching distinct values for column {column_name} in table {table_name}: {e}")

            cursor.close()

        except Exception as e:
            print(f"Error retrieving table metadata: {e}")

        # Step 5: Write out to JSON file
        try:
            file_name = os.path.join(folder_path, "column_info.json")
            with open(file_name, 'w', encoding='utf-8') as json_file:
                json.dump(table_metadata, json_file, ensure_ascii=False, indent=4, default=self.custom_json_serializer)
            print(f"Table data successfully saved to {file_name}")
        except Exception as e:
            print(f"Error writing to JSON file: {e}")


    @staticmethod
    def custom_json_serializer(obj):
        """ Custom converter for Decimal, date, and datetime objects to be JSON serializable """
        if isinstance(obj, decimal.Decimal):
            return float(obj)  # or str(obj) if you prefer to keep precision as a string
        elif isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()  # Convert date or datetime to ISO 8601 string
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")