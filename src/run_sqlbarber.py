from db_controller.factory import create_db_controller
from llm.gpt import GPT
import sys, os, json
from pathlib import Path
from sqlbarber.prompts import SQL_GENERATION_TEMPLATE # this prompt template would only be used by NaiveSQLTemplateGenerator as a simple baseline, we use AdvancedSQLTemplateGenerator in SQLBarber
from sqlbarber.runner import SQLBarberRunner
from pathlib import Path

# user provides sql requirement and optimization constraint
para = sys.argv

cost_type = para[1]
distribution = para[2]
total_sqls = int(para[3])
min_cost = int(para[4])
max_cost = int(para[5])
num_intervals = int(para[6])
num_iterations = int(para[7])
dbname = str(para[8])

summary_name = f"{dbname}_{cost_type}_{min_cost}_{max_cost}_{num_intervals}_{distribution}"

# DBController: create an instance of PostgreSQLController from configuration file
target_dbms = "postgres" 
config_path = "./configs/postgres.ini" 
db_controller = create_db_controller(target_dbms, config_path)
db_controller._connect(dbname)

task_name = f"{target_dbms}_{dbname}"

# prepare the DB column information, this only need to be done one time for each database
column_info_folder = f"{Path(__file__).resolve().parents[1]}/outputs/intermediate/db_meta_info/{task_name}/"
if not os.path.exists(f"{column_info_folder}column_info.json"):
    print(f"--- Column Information for {task_name} is not available, trying to get this information from the database. ---")
    print("This could take some time, depending on the size of the database. But this only need to be done for one time and reused in the future for a given database.")
    print("If there are significant changes to a database, please delete the file and re-execute this command.")
    db_controller.get_column_info(column_info_folder)
else:
    print(f"DB column information loaded successfully from {column_info_folder}")

# user specify which LLM to invoke
try:
    api_key = os.environ['OPENAI_API_KEY']
    print("API key loaded successfully")
except KeyError:
    print("OPENAI_API_KEY not found in environment variables")
    api_key = None
model = "o3-mini"
gpt = GPT(api_key=api_key, model=model)

# user provides semantic requirements
semantic_requirements = []
semantic_requirements.append([3, "The query should have a nested query with aggregation, at least two predicate values to fill."])
semantic_requirements.append([3, "The query should use aggregation, and have at least three predicate values to fill."])
semantic_requirements.append([3, "The query should use group-by, and have at least two predicate values to fill."])

template_generators = ["Naive", "Advanced"]
template_generator = template_generators[1]

# create SQLBarber Runner to use SQLBarber based on user requirement
sqlbarber_runner = SQLBarberRunner(
    task_name,
        gpt,
            template_generator, 
                db_controller, 
                    semantic_requirements, 
                        total_sqls,
                            min_cost,
                                max_cost,
                                    num_intervals,
                                        target=cost_type,
                                            summary_name=summary_name)

# target sql distribution generation
with open(f'{Path(__file__).resolve().parents[1]}/benchmark/query_cost_distribution/cost_distributions.json', 'r') as f:
    target_distributions = json.load(f)
target_distribution = target_distributions[distribution]
sqlbarber_runner.generate_target_sql_distribution(distribution, interval_counts=target_distribution)

# SQLBarber generates SQL satisfying user requirement
sqlbarber_runner.generate_sql(
    SQL_GENERATION_TEMPLATE, 
        semantic_requirements, 
            num_iterations, 
                num_profiling = int(0.15 * total_sqls),
                    generate_new_sql_tamplate=True,
                        reuse_history=True)