import os, json, re, time
from smac import HyperparameterOptimizationFacade, Scenario, initial_design
from ConfigSpace import (
    ConfigurationSpace,
    OrdinalHyperparameter,
)
import json
import numpy as np
import os
import matplotlib.pyplot as plt
from smac.runhistory.runhistory import RunHistory
from collections import OrderedDict
from pathlib import Path
from .cpu_cost_calculator import CPUCostCalculator

class PredicateEnumerator:
    def __init__(self, task_name, db_controller, template_id, sql_template, target_cost, file_path, seed=1, target="cost", cost_type="sum_cost"):
        """
            Args:
                target: can be "card", "cost" or "time"
        """
        self.cost_type = cost_type
        self.task_name = task_name
        self.db_controller = db_controller
        self.template_id = template_id
        self.sql_template = sql_template
        self.target_cost = target_cost
        self.seed = seed
        self.value_mapping = {}
        self.cost_history = {}
        self.sql_execute_time = 0
        self.search_space = ConfigurationSpace()
        self.column_info = self.load_table_data_from_json(file_path)

        self.supported_targets = ["card", "cost", "time", "cpu"]
        if target not in self.supported_targets:
            raise ValueError(f"Invalid target '{target}'. Must be one of {self.supported_targets}.")
        self.target = target

        # Initialize CPU cost calculator for cpu target
        if self.target == "cpu":
            self.cpu_cost_calculator = CPUCostCalculator(self.db_controller)

        self._root = Path(__file__).resolve().parents[2]
        self.result_path = f"{self._root}/outputs/intermediate/result_visualization/initial_sampling/{self.task_name}/initial_sampling_{self.template_id}_histogram.png"
        os.makedirs(os.path.dirname(self.result_path), exist_ok=True)

        self.cost_history_path = f"{self._root}/outputs/intermediate/cost_history/{self.target}/{self.task_name}"

        self.queries = []
        self.costs = []

    def load_table_data_from_json(self, file_path):
        """ Load table metadata from a JSON file """
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as json_file:
                    table_data = json.load(json_file)
                    return table_data
            except Exception as e:
                print(f"Error reading JSON file: {e}")
                return None

    def identify_placeholders(self):
        """
        1. Reads the table_metadata from the JSON file (e.g. 'column_info.json').
        2. Extracts placeholders from the SQL template in the format {{table_name.column_name}}.
        3. Allows two valid suffixes for columns: '_start' or '_end'.
        4. For any other suffix (e.g. '_min', '_foo', etc.), we repeatedly remove the trailing underscore-part
        and test if the resulting base column name is valid. If yes, we adopt that base column name.
        5. Returns a list of valid placeholders: ["table.column", ...].
        """
        import re

        # --- Step 1: Load table_metadata from JSON file (or however it's supplied) ---
        table_metadata = self.column_info
            
        # --- Step 2: Extract placeholders from self.sql_template ---
        # e.g. placeholders might look like ["orders.o_totalprice_min", "orders.o_custkey_start", ...]
        placeholder_pattern = re.compile(r"'?{{(\w+\.\w+)}}'?")
        raw_placeholders = re.findall(placeholder_pattern, self.sql_template)

        valid_placeholders = []

        for placeholder in raw_placeholders:
            # Split "table_name.column_name"
            if '.' not in placeholder:
                continue
            table_name, col = placeholder.split('.', 1)

            # --- Step 3: Validate table_name ---
            if table_name not in table_metadata:
                # Skip if table not found in metadata
                continue
            
            valid_columns_for_table = set(table_metadata[table_name].keys())

            # --- Step 4: Handle special suffixes: _start / _end ---
            if col.endswith('_start'):
                base_col = col[:-6]  # remove "_start"
                if base_col in valid_columns_for_table:
                    # e.g. "orders.o_totalprice_start"
                    valid_placeholders.append(f"{table_name}.{col}")
                # If base_col not valid, skip
                continue

            if col.endswith('_end'):
                base_col = col[:-4]  # remove "_end"
                if base_col in valid_columns_for_table:
                    valid_placeholders.append(f"{table_name}.{col}")
                # If base_col not valid, skip
                continue

            # --- Step 5: For any other suffix (e.g. "_min", "_foo", "_min_max", etc.) ---
            # repeatedly strip the trailing underscore block until we (1) find a valid base_col or (2) run out
            while '_' in col and col not in valid_columns_for_table:
                # Remove everything after the last underscore
                col = col[:col.rfind('_')]

            # Now final check if this column is valid
            if col in valid_columns_for_table:
                valid_placeholders.append(f"{table_name}.{col}")
            # else skip

        return valid_placeholders
    
    def get_distinct_values(self):
        """
        Get the distinct values of each placeholder from the precomputed JSON file
        (rather than running new SQL queries).
        Store the mapping {placeholder -> {str_value -> ori_value}} in self.value_mapping.
        Returns {placeholder: [list_of_distinct_values]}
        """
        # Load the precomputed column info from JSON
        table_metadata = self.column_info

        placeholders = self.identify_placeholders()
        all_distinct_values = {}

        for placeholder in placeholders:
            # e.g. "table_name.column_name", or "table_name.column_name_start"
            table_name, column_name_with_suffix = placeholder.split('.')

            # handle suffix: _start and _end
            if column_name_with_suffix.endswith('_start'):
                column_name = column_name_with_suffix[:-6]
            elif column_name_with_suffix.endswith('_end'):
                column_name = column_name_with_suffix[:-4]
            else:
                column_name = column_name_with_suffix

            # Safeguards: ensure table and column exist in table_metadata
            if table_name not in table_metadata or column_name not in table_metadata[table_name]:
                # Not found, skip or handle error
                continue

            # Read the pre-sampled distinct values from JSON
            distinct_vals = table_metadata[table_name][column_name].get('sampled_distinct_values', [])

            all_distinct_values[placeholder] = distinct_vals

            # Build {str_value -> ori_value} mapping
            self.value_mapping[placeholder] = {
                str(value): value for value in distinct_vals
            }

        return all_distinct_values

    def define_search_space(self):
        """
            define search space, turn predicate values into ordinalhyperparameters
        """

        columns_values = self.get_distinct_values()

        for placeholder, distinct_values in columns_values.items():
            table_name, column_name_with_suffix = placeholder.split('.')

            if column_name_with_suffix.endswith('_start'):
                column_name = column_name_with_suffix[:-6]
            elif column_name_with_suffix.endswith('_end'):
                column_name = column_name_with_suffix[:-4]
            else:
                column_name = column_name_with_suffix

            if table_name in self.column_info and column_name in self.column_info[table_name]:
                column_meta = self.column_info[table_name][column_name]
                column_type = column_meta['type']

                if column_type in ['integer', 'bigint', 'smallint', 'float', 'double precision', 'numeric', 'real']:
                    # numerical columns
                    if distinct_values:
                        sorted_distinct_values = sorted(distinct_values)
                        hyperparameter = OrdinalHyperparameter(
                            name=placeholder, sequence=[str(value) for value in sorted_distinct_values]
                        )
                        self.search_space.add_hyperparameter(hyperparameter)

                elif column_type in ['text', 'varchar', 'boolean', 'character', 'character varying', 'date']:
                    # non-numerical columns
                    if distinct_values:
                        hyperparameter = OrdinalHyperparameter(
                            name=placeholder, sequence=[str(value) for value in distinct_values]
                        )
                        self.search_space.add_hyperparameter(hyperparameter)
  
    def set_and_replay(self, config, seed=0):
        """
        Generate a SQL query based on the predicate values and the SQL template, 
        then estimate the cost (number of estimated rows) using DBMS statistics.
        
        Args:
            config: A ConfigSpace configuration object containing predicate values.
            seed: Random seed (optional).
        
        Returns:
            A score that represents the similarity between target_cost (or range) 
            and estimated_cost. The score is transformed to a format suitable for 
            Bayesian optimization (minimization).
        """

        sql_template = self.sql_template
        values = []

        # Prepare the final SQL query by replacing placeholders with actual values from `config`
        final_query = sql_template
        for placeholder in config:
            if placeholder.endswith('start') or placeholder.endswith('end'):
                if placeholder.endswith('start'):
                    base_name = placeholder[:-6]
                    another_name = f"{base_name}_end"
                else:
                    base_name = placeholder[:-4]
                    another_name = f"{base_name}_start"

                one_value = config[placeholder]
                one_value = self.value_mapping[placeholder].get(one_value, one_value)

                another_value = config[another_name]
                another_value = self.value_mapping[another_name].get(another_value, another_value)

                if placeholder.endswith('start'):
                    value = min(one_value, another_value)
                else:
                    value = max(one_value, another_value)
            else:
                value = config[placeholder]
                value = self.value_mapping[placeholder].get(value, value)

            if isinstance(value, str):
                value = value.strip()
                values.append(value)
                final_query = final_query.replace(f"{{{{{placeholder}}}}}", value)
            else:
                values.append(value)
                final_query = final_query.replace(f"{{{{{placeholder}}}}}", str(value))
        
        # Use EXPLAIN to get the estimated cost
        explain_query = f"EXPLAIN {final_query};"
        execute_query = f"{final_query};"

        try:
            estimated_costs = []

            if self.target == "card":
                # Execute the EXPLAIN query to get the execution plan
                start_time = time.time()
                result = self.db_controller.execute_sql(explain_query)["result"]
                end_time = time.time()
                self.sql_execute_time += (end_time - start_time) / 60

                # Parse the result to extract the estimated number of rows (cost)
                if result and len(result) > 0:
                    for line in result:
                        explain_line = line[0]  
                        match = re.search(r'rows=(\d+)', explain_line)
                        if match:
                            estimated_costs.append(int(match.group(1)))

            elif self.target == "cost":
                # Execute the EXPLAIN query to get the execution plan
                start_time = time.time()
                result = self.db_controller.execute_sql(explain_query)["result"]
                end_time = time.time()
                self.sql_execute_time += (end_time - start_time) / 60

                # Parse the result to extract the total cost for each relevant line
                if result and len(result) > 0:
                    for line in result:
                        explain_line = line[0]  # Assume the line text is in the first element
                        # Use regex to match the cost pattern and capture the total cost after ".."
                        match = re.search(r'cost=\d+\.\d+\.\.(\d+\.\d+)', explain_line)
                        if match:
                            estimated_costs.append(float(match.group(1)))  # Extract and store the total cost as a float

            elif self.target == "time":
            # Execute the query to get the execution time
                start_time = time.time()
                result = self.db_controller.execute_sql(execute_query)["result"]
                end_time = time.time()
                sql_execution_time = end_time - start_time
                estimated_costs.append(sql_execution_time)

            elif self.target == "cpu":
            # Calculate CPU cost using the CPU cost calculator
                start_time = time.time()
                cpu_cost = self.cpu_cost_calculator.calculate_cpu_cost(final_query)
                end_time = time.time()
                self.sql_execute_time += (end_time - start_time) / 60

                if cpu_cost is not None:
                    estimated_costs.append(cpu_cost)
                else:
                    # If CPU cost calculation fails, return high penalty
                    return 1.0

            self.cost_history[final_query] = estimated_costs
            estimated_cost = self.calculate_cost(estimated_costs)
            self.queries.append(final_query)
            self.costs.append(estimated_cost)

            # Handle single value vs range target_cost
            transformed_score = self.calculate_performance(self.target_cost, estimated_cost)

            return transformed_score

        except Exception as e:
            print(f"Error during cost estimation using EXPLAIN: {e}")
            return 1.0  # Return a high value on error to minimize in Bayesian optimization

    def reuse_history(self):

        # Create a new runhistory to store all modified values
        new_runhistory = RunHistory()

        # Source 1: Load original_runhistory from the initial_sampling file
        initial_sampling_file = f"{self._root}/outputs/intermediate/smac3_output/{self.task_name}_{self.target}_initial_sampling_{self.template_id}/runhistory.json"
        if os.path.exists(initial_sampling_file):
            original_runhistory = RunHistory()
            original_runhistory.update_from_json(initial_sampling_file, self.search_space)
            
            costs = self.read_cost(f"{self.cost_history_path}/initial_sampling_{self.template_id}.json")
            
            for idx, (trial_key, trial_value) in enumerate(original_runhistory.items()):
                config = original_runhistory.ids_config[trial_key.config_id]
                estimated_cost = costs[idx] if costs and idx < len(costs) else None
                new_performance = self.calculate_performance(self.target_cost, estimated_cost)
                new_runhistory.add(config=config, cost=new_performance)

        # Source 2: Find all directories with a specific prefix and load runhistory.json from them
        base_dir = f"{self._root}/outputs/intermediate/smac3_output"
        for folder in os.listdir(base_dir):
            folder_path = os.path.join(base_dir, folder)
            if os.path.isdir(folder_path) and folder.startswith(f"{self.task_name}_{self.template_id}_{self.target}"):
                
                runhistory_file = os.path.join(folder_path, "runhistory.json")
                if os.path.exists(runhistory_file):
                    original_runhistory = RunHistory()
                    original_runhistory.update_from_json(runhistory_file, self.search_space)
                    
                    cost_file = f"{self.cost_history_path}/{self.template_id}_{self.target_cost[0]}_to_{self.target_cost[1]}.json"
                    costs = self.read_cost(cost_file)
                    
                    for idx, (trial_key, trial_value) in enumerate(original_runhistory.items()):
                        config = original_runhistory.ids_config[trial_key.config_id]
                        estimated_cost = costs[idx] if costs and idx < len(costs) else None
                        new_performance = self.calculate_performance(self.target_cost, estimated_cost)
                        new_runhistory.add(config=config, cost=new_performance)

        return new_runhistory
    
    def calculate_performance(self, target_cost, estimated_cost):
        if estimated_cost is None:
            # If no cost data is available, return a bad performance
            return 1

        # Handle single value vs range target_cost
        if isinstance(target_cost, list) and len(target_cost) == 2:
            c_l, c_r = target_cost
            if c_l <= estimated_cost <= c_r:
                reward = 1
            else:
                # Calculate similarity to range bounds
                delta_l = min(estimated_cost / c_l, c_l / estimated_cost)
                delta_r = min(estimated_cost / c_r, c_r / estimated_cost)
                reward = max(delta_l, delta_r)
            
            transformed_score = 1 - reward
        else:
            # Single value case
            similarity_score = min(target_cost, estimated_cost) / max(target_cost, estimated_cost)
            transformed_score = 1 - similarity_score

        # Return the transformed score as the new performance (could adjust scaling if necessary)
        return transformed_score

    def optimize(self, name, trials_number, initial_config_number, reuse_history=True):
        retrain_after = 20
        retries = 50
        self.cost_history = {}
        self.define_search_space()

        space_size = self.search_space.estimate_size()

        # initial profiling
        if trials_number == initial_config_number + 1:
            runhistory = None
            if space_size < trials_number:
                trials_number = space_size
                initial_config_number = space_size - 1
            retrain_after = trials_number # don't train the surrogate model in profiling stage
        else:
            # bayesian optimization-based predicate value enumeration
            if reuse_history:
                runhistory = self.reuse_history()
                trials_number = len(runhistory) + trials_number
                initial_config_number = 0 # we have sampled many points in profiling stage, no need now
            else:
                runhistory = None

            if space_size < trials_number:
                history_length = len(runhistory) if runhistory is not None else 0
                trials_number = space_size - history_length
                initial_config_number = int(0.2 * trials_number) if not reuse_history else 0

        scenario = Scenario(
            configspace=self.search_space,
            name=name,
            seed=self.seed,
            deterministic=True,
            n_trials=trials_number,
            use_default_config=True,
            output_directory=f"{self._root}/outputs/intermediate/smac3_output"
        )
        init_design = initial_design.LatinHypercubeInitialDesign(
            scenario,
            n_configs=initial_config_number,
            max_ratio=1,  # set this to a value close to 1 to get exact initial_configs as specified
        )

        """
            HyperparameterOptimizationFacade uses random forest as surrogate model
            however there is an implementation issue: only eupport categorical parameters with only 128 values
            https://github.com/automl/SMAC3/issues/504
            so we use ordinalhyperparameter
        """
        smac = HyperparameterOptimizationFacade(  
            config_selector=HyperparameterOptimizationFacade.get_config_selector(scenario, retrain_after=retrain_after, retries=retries),
            scenario=scenario,
            initial_design=init_design,
            target_function=self.set_and_replay,
            overwrite=True,
        )

        if runhistory is not None:
            # Sort the runhistory by cost
            sorted_runhistory = sorted(runhistory.items(), key=lambda x: x[1].cost)

            # Select the top n configs with the lowest cost
            n = int(len(runhistory) * 0.25)
            lowest_n_configs = sorted_runhistory[:n]

            # Add the selected configs to the new runhistory
            for idx, (trial_key, trial_value) in enumerate(lowest_n_configs):
                config = runhistory.ids_config[trial_key.config_id]
                smac.runhistory.add(config=config, cost=trial_value.cost)

        smac.optimize()

        if trials_number == initial_config_number + 1:
            self.store_costs(f"{self.cost_history_path}", "initial_sampling")
            new_costs = None
        else:
            new_costs = self.store_costs(f"{self.cost_history_path}")

        return new_costs, space_size - trials_number # return the costs of newly generated quereis and the remainng space size

    def store_costs(self, folder_path, prefix_name=None):
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        if prefix_name is not None:
            file_name = os.path.join(folder_path, f"{prefix_name}_{self.template_id}.json")
        else:
            if isinstance(self.target_cost, list) and len(self.target_cost) == 2:
                file_name = os.path.join(folder_path, f"{self.template_id}_{self.target_cost[0]}_to_{self.target_cost[1]}.json")
            else:
                file_name = os.path.join(folder_path, f"{self.template_id}_{self.target_cost}.json")

        # Check if the file already exists
        if os.path.exists(file_name):
            # If the file exists, read the existing content
            with open(file_name, 'r', encoding='utf-8') as json_file:
                try:
                    existing_data = json.load(json_file, object_pairs_hook=OrderedDict)
                except json.JSONDecodeError:
                    existing_data = OrderedDict()  # Initialize as empty OrderedDict if the file is empty or corrupted
        else:
            existing_data = OrderedDict()

        # Merge existing content with new data
        if self.cost_history is not None:
            new_costs = [value for key, value in self.cost_history.items() if key not in existing_data]

            # Ensure that existing data is updated, keeping the order
            for key, value in self.cost_history.items():
                existing_data[key] = value  # Update the value or add new key-value pairs at the end

            # Write the updated content back to the file with preserved order
            with open(file_name, 'w', encoding='utf-8') as json_file:
                json.dump(existing_data, json_file, ensure_ascii=False, indent=4)

            return new_costs

    def analyze_template(self, num_samplings):
        self.optimize(f"{self.task_name}_{self.target}_initial_sampling_{self.template_id}", num_samplings, num_samplings-1)

        file_path = f"{self.cost_history_path}/initial_sampling_{self.template_id}.json"
        final_costs = self.read_cost(file_path)

        if final_costs is not None and final_costs != []:
            num_intervals = 20
            intervals, interval_frequencies = self.calculate_intervals_frequency(final_costs, num_intervals)
            strength_regions = self.identify_strength_regions(intervals, interval_frequencies)

            self.draw_sampling_histogram(intervals, interval_frequencies, strength_regions)

            return final_costs
        else:
            return None
 
    # Function to divide the range [min, max] into 'num_intervals' and count frequencies
    def calculate_intervals_frequency(self, elements, num_intervals):

        new_elements = []
        for element in elements:
            if element is not None:
                new_elements.append(element)
        elements = new_elements
        
        min_value = min(elements)
        max_value = max(elements)
        
        # Create intervals using numpy's linspace
        intervals = np.linspace(min_value, max_value, num_intervals+1)
        
        # Count the frequency of elements falling into each interval
        interval_counts = np.histogram(elements, bins=intervals)[0]
        
        # Normalize the counts to get frequencies
        total_elements = len(elements)
        interval_frequencies = interval_counts / total_elements
        
        return intervals, interval_frequencies

    # Function to identify "strength" regions (highest frequency intervals)
    def identify_strength_regions(self, intervals, interval_frequencies, threshold_type='mean', threshold_ratio=1.5, percentile=None):
        """
        Identify strength regions based on different threshold types.
        
        Parameters:
        - intervals: list of interval edges
        - interval_frequencies: list of frequencies corresponding to each interval
        - threshold_ratio: the ratio to determine the threshold for strength regions
        - threshold_type: 'mean', 'median', or 'percentile' to specify which threshold to use
        
        Returns:
        - List of strength regions
        """
        if threshold_type == 'mean':
            threshold = np.mean(interval_frequencies) * threshold_ratio
        elif threshold_type == 'median':
            threshold = np.median(interval_frequencies) * threshold_ratio
        elif threshold_type == 'percentile':
            threshold = np.percentile(interval_frequencies, percentile) * threshold_ratio
        else:
            raise ValueError(f"Unknown threshold type: {threshold_type}")
        
        # Find regions where frequency is significantly higher than the threshold
        strength_regions = []
        for i, freq in enumerate(interval_frequencies):
            if freq >= threshold:
                strength_regions.append((intervals[i], intervals[i+1], freq))
        
        return strength_regions

    def draw_sampling_histogram(self, intervals, interval_frequencies, strength_regions):
        # Convert intervals to bin labels (midpoints of intervals)
        bin_labels = [(intervals[i] + intervals[i + 1]) / 2 for i in range(len(intervals) - 1)]
        
        # Extract the start and end of strength regions for coloring
        strength_bins = [(region[0], region[1]) for region in strength_regions]

        # Plot histogram
        plt.figure(figsize=(5, 2))
        bars = plt.bar(bin_labels, interval_frequencies, width=np.diff(intervals)[0], edgecolor='black')

        # Highlight strength regions with a different color
        for bar, bin_label in zip(bars, bin_labels):
            for region_start, region_end in strength_bins:
                if region_start <= bin_label < region_end:
                    bar.set_color('red')  # Highlight strength regions in red
                    bar.set_edgecolor('black')
    
        # Finalize plot with labels and title
        plt.xlabel('Regions (Midpoints of Intervals)')
        plt.ylabel('Frequency')
        plt.title(f'Frequency Distribution for {self.template_id}')
        # plt.legend(loc='upper right')

        # Save plot as PNG
        result_path = self.result_path
        
        plt.savefig(result_path)
        plt.close()

    def calculate_cost(self, costs):
        cost_type = self.cost_type

        if costs == []:
            return None

        if cost_type == "output_cost":
            return costs[0]   
        elif cost_type == "sum_cost":
            return sum(costs)     
        else:
            raise ValueError(f"Invalid cost_type: {cost_type}")

    def read_cost(self, file_path):
        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                data = json.load(file)

            costs = []
            for key, values in data.items():
                costs.append(self.calculate_cost(values))  # final cost

            all_none = True
            for cost in costs:
                if cost is not None:
                    all_none = False

            if all_none:
                return None
            else:
                return costs
        else:
            return None