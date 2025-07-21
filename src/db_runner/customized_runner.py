from db_controller.base_controller import BaseDBController
import time

class CustomizedRunner:
    def __init__(self, db_controller: BaseDBController, target_database):
        self.dbms = db_controller
        self.runner_type = "customized"
        self.target_database = target_database
    
    def run_benchmark(self, target_benchmark):
        dbms = self.dbms
        dbms._connect(self.target_database)
        
        start_ms = time.time() * 1000.0
        flag = dbms.execute_queries(target_benchmark)
        end_ms = time.time() * 1000.0
        execution_time = end_ms - start_ms

        dbms._connect("postgres")

        if flag:
            return execution_time
        else:
            return -1