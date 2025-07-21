from db_runner.benchbase_runner import BenchbaseRunner
from db_runner.customized_runner import CustomizedRunner
from db_controller.base_controller import BaseDBController

def create_db_runner(target_runner, db_controller: BaseDBController, target_path="./optimization_results/temp_results", target_database="benchbase"):
    if target_runner == "benchbase":
        db_runner = BenchbaseRunner(db_controller, target_path)
    else:
        db_runner = CustomizedRunner(db_controller, target_database)

    return db_runner