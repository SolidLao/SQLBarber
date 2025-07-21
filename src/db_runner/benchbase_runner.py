import subprocess
import os
import glob
import json
from db_controller.base_controller import BaseDBController

class BenchbaseRunner:
    def __init__(self, db_controller: BaseDBController, target_path):
        """target_path is the relative path under the GPTuner folder"""
        self.process = None
        self.dbms = db_controller
        self.target_path = target_path
        self.runner_type = "benchbase"

        if not os.path.exists("./benchbase"):
            os.system("./scripts/install_benchbase.sh")

        if self.dbms.name == "postgres":  
            self.benchmark_path = "./benchbase/target/benchbase-postgres"
        elif self.dbms.name == "mysql": 
            self.benchmark_path = "./benchbase/target/benchbase-mysql"

    def check_sequence_in_file(self):
        file_path = os.path.join(self.target_path, "out.txt")
        try:
            with open(file_path, 'r') as file:
                content = file.read()
                error_index = content.find("Unexpected SQL Errors")
                if error_index != -1:
                    for i in range(1, 23):
                        formatted_num = "%02d" % i
                        if f"Q{i}/{formatted_num}" in content[error_index:]:#？
                            return True
                else:
                    return False
        except FileNotFoundError:
            print(f"File Not Found: {file_path}")
            return False
        except Exception as e:
            print(f"Error: {e}")
            return False

    def run_benchmark(self, target_benchmark):
        # store the optimization results
        folder_paths = ["./optimization_results/", "./optimization_results/temp_results"]
        for folder_path in folder_paths:
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)  

        with open(os.path.join(self.target_path, "out.txt"), 'w') as output_file:
            if self.dbms.name == "postgres":  
                self.process = subprocess.Popen(    
                    ['java', '-jar', 'benchbase.jar', '-b', target_benchmark, 
                    "-c", "config/postgres/sample_{}_config.xml".format(target_benchmark), 
                    "--create=false", "--clear=false", "--load=false", '--execute=true', 
                    "-d", os.path.join("../../../", self.target_path)],
                    cwd=self.benchmark_path,
                    stdout=output_file
                )

            elif self.dbms.name == "mysql":
                self.process = subprocess.Popen(
                    ['java', '-jar', 'benchbase.jar', '-b', target_benchmark, 
                    "-c", "config/mysql/sample_{}_config.xml".format(target_benchmark), 
                    "--create=false", "--clear=false", "--load=false", '--execute=true', 
                    "-d", os.path.join("../../../", self.target_path)],
                    cwd=self.benchmark_path,
                    stdout=output_file
                )
            self.process.wait()

    def clear_summary_dir(self):
        for filename in os.listdir(self.target_path):
            print(f"REMOVE {filename}")
            filepath = os.path.join(self.target_path, filename)
            os.remove(filepath)

    def get_latest_summary_file(self):
        files = glob.glob(os.path.join(self.target_path, '*summary.json'))
        files.sort(key=os.path.getmtime, reverse=True)  
        return files[0] if files else None

    def get_latest_raw_file(self):
        files = glob.glob(os.path.join(self.target_path, '*raw.csv'))
        files.sort(key=os.path.getmtime, reverse=True)  
        return files[0] if files else None

    def get_throughput(self):
        summary_file = self.get_latest_summary_file()
        try:
            with open(summary_file, 'r') as file:
                data = json.load(file)
            throughput = data["Throughput (requests/second)"]
            if throughput==-1 or throughput == 2147483647: # human error: if process is terminated by ''ctrl+c'' or we forget to return something in func “set_and_play”, SMAC3 will return this value 
                raise ValueError(f"Benchbase return error throughput:{throughput}")
            print(f"Throughput: {throughput}")
        except Exception as e:
            print(f'Exception for JSON: {e}')
        return throughput

    def get_latency(self):
        summary_file = self.get_latest_summary_file()
        try:
            with open(summary_file, 'r') as file:
                data = json.load(file)
            average_latency = data["Latency Distribution"]["Average Latency (microseconds)"]
            if average_latency == -1 or average_latency == 2147483647:
                raise ValueError(f"Benchbase return error average_latency:{average_latency}")
            print(f"Latency: {average_latency}")
        except Exception as e:
            print(f'Exception for JSON: {e}')
        return average_latency