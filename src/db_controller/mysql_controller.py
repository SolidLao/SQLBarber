from db_controller.base_controller import BaseDBController
import mysql.connector
import os
import time

class MySQLController(BaseDBController):
    """ Instantiate DBMSTemplate to support PostgreSQL DBMS """
    def __init__(self, db, user, password, restart_cmd, recover_script, port):
        super().__init__(db, user, password, restart_cmd, recover_script, port)
        self.name = "mysql"
        self.global_vars = [t[0] for t in self.query_all(
            'show global variables') if self.is_numerical(t[1])]
        self.server_cost_params = [t[0] for t in self.query_all(
            'select cost_name from mysql.server_cost')]
        self.engine_cost_params = [t[0] for t in self.query_all(
            'select cost_name from mysql.engine_cost')]
        self.all_variables = self.global_vars + \
            self.server_cost_params + self.engine_cost_params
    
    def _connect(self, db=None):
        self.failed_times = 0
        if db==None:
            db=self.db
        print(f'Trying to connect to {db} with user {self.user}')
        while True:
            try:
                self.connection = mysql.connector.connect(
                    database=db,
                    user=self.user,
                    password=self.password,
                    host="localhost"
                )
                print(f"Success to connect to {db} with user {self.user}")
                return True
            except Exception as e:
                self.failed_times += 1
                print(f'Exception while trying to connect: {e}')
                if self.failed_times <= 4:
                    self.recover_dbms()
                    print("Reconnet again")
                else:
                    return False
                time.sleep(3)

            
    def _disconnect(self):
        if self.connection:
            print('Disconnecting ...')
            self.connection.close()
            print('Disconnecting done ...')
            self.connection = None
    
    def _copy_db(self, source_db, target_db):
        ms_clc_prefix = f'mysql -u{self.user} -p{self.password} '
        ms_dump_prefix = f'mysqldump -u{self.user} -p{self.password} '
        os.system(ms_dump_prefix + f' {source_db} > copy_db_dump')
        print('Dumped old database')
        os.system(ms_clc_prefix + f" -e 'drop database if exists {target_db}'")
        print('Dropped old database')
        os.system(ms_clc_prefix + f" -e 'create database {target_db}'")
        print('Created new database')
        os.system(ms_clc_prefix + f" {target_db} < copy_db_dump")
        print('Initialized new database')

    def query_all(self, sql):
        try:
            cursor = self.connection.cursor(buffered=True)
            cursor.execute(sql)
            results = cursor.fetchall()
            cursor.close()
            return results
        except Exception as e:
            print(f'Exception in mysql.query_all: {e}')
            return None
        
    def execute_queries(self, target_workload):
        
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
            cursor = self.connection.cursor(buffered=True)
            cursor.execute(sql, multi=True)
            cursor.close()
            return True
        except Exception as e:
            print(f"Failed to execute {sql} to update dbms for error: {e}")
            return False 