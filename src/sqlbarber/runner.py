import numpy as np
import matplotlib.pyplot as plt
import os
from .predicate_enumerator import PredicateEnumerator
from .template_generator import NaiveSQLTemplateGenerator, AdvancedSQLTemplateGenerator
from .utils import timing_decorator
import json
from datetime import datetime
import sqlparse, re
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from scipy.stats import wasserstein_distance
import traceback
from pathlib import Path
from collections import defaultdict

class SQLBarberRunner:
    def __init__(self, task_name, gpt, template_generator, db_controller, semantic_requirements, total_sqls, min_cost, max_cost, num_intervals=10, target="cost", cost_type="sum_cost", summary_name=None):
        self.ori_task_name = task_name
        self.task_name = task_name + "_" + datetime.now().strftime("%Y-%m-%d_%H-%M")
        self.gpt = gpt
        self.semantic_requirements = semantic_requirements
        self.db_controller = db_controller
        self.total_sqls = total_sqls
        self.min_cost = min_cost
        self.max_cost = max_cost
        self.num_intervals = num_intervals
        self.target_distribution = None
        self.current_distribution = [0 for _ in range(num_intervals)]
        self.template_ids = None
        self.templates = None
        self.missing_intervals = []
        self.target = target
        if self.target == "card":
            self.cost_type = "sum_cost"
        elif self.target == "cost" or self.target == "time" or self.target == "cpu":
            self.cost_type = "output_cost"
        self.summary_name = summary_name

        self.template_generator = self.init_template_generator(template_generator, task_name)

        # Track bad combinations of (interval, template_id)
        self.bad_combinations = set()

        # A dictionary to store the remaining space for each template
        self.template_remaining_spaces = {}

        # Track how many times an interval is selected for optimization
        self.selected_times_of_intervals = [0 for _ in range(num_intervals)]

        self._root = Path(__file__).resolve().parents[2]
        self.column_info_path = f"{self._root}/outputs/intermediate/db_meta_info/{self.ori_task_name}/column_info.json"
        self.seed_template_path = f"{self._root}/outputs/final/sql_template/{self.ori_task_name}"
        os.makedirs(os.path.dirname(self.seed_template_path), exist_ok=True)

        # Log file setup
        self.log_file = os.path.join(f"{self._root}/outputs/intermediate/logs/{self.task_name}", f"process.log")
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)

        self.queries = []
        self.costs = []

        # Performance summary file setup
        self.workload_file = os.path.join(f"{self._root}/outputs/final/{self.task_name}/{self.summary_name}", "workload.json")
        self.summary_file = os.path.join(f"{self._root}/outputs/final/{self.task_name}/{self.summary_name}", "summary.json")
        os.makedirs(os.path.dirname(self.workload_file), exist_ok=True)
        os.makedirs(os.path.dirname(self.summary_file), exist_ok=True)

    def init_template_generator(self, template_generator, task_name):
        if template_generator == "Naive":
            sql_template_generator = NaiveSQLTemplateGenerator(task_name, self.db_controller, self.gpt)
        else:
            sql_template_generator = AdvancedSQLTemplateGenerator(task_name, self.db_controller, self.gpt)

        return sql_template_generator

    def log(self, message):
        # Append message to log file
        with open(self.log_file, "a") as log_file:
            log_file.write(message + "\n")
 
    def generate_target_sql_distribution(self, distribution='uniform', user_data=None, interval_counts=None):
        """
        Generate SQL query cost distribution and return the counts of queries in each interval.
        Args:
            distribution (str): Distribution type, can be 'normal', 'uniform', 'exponential'
            user_data (list): User-provided sample data to generate the distribution
            interval_counts (list): User-specified number of SQL queries per interval

        Returns:
            np.array: Counts of SQL queries in each interval
        """
        if user_data is not None:
            self.log(f"The target sql distribution is based on user data: {user_data}")
        elif interval_counts is not None:
            self.log(f"The target sql distribution is based on interval_counts: {interval_counts}")
        else:
            self.log(f"The target sql distribution is based on distribution: {distribution}")

        # Ensure that total_sqls, min_cost, max_cost, and num_intervals are properly initialized
        total_sqls = getattr(self, 'total_sqls', None)
        min_cost = getattr(self, 'min_cost', None)
        max_cost = getattr(self, 'max_cost', None)
        num_intervals = getattr(self, 'num_intervals', None)

        self.log(f"Generate {total_sqls} SQL, with cost from {min_cost} to {max_cost}, split into {num_intervals} intervals.")

        if total_sqls is None or min_cost is None or max_cost is None or num_intervals is None:
            raise ValueError("Attributes 'total_sqls', 'min_cost', 'max_cost', and 'num_intervals' must be initialized.")

        if min_cost >= max_cost:
            raise ValueError("Attribute 'min_cost' must be less than 'max_cost'.")

        intervals = np.linspace(min_cost, max_cost, num_intervals + 1)

        if user_data is not None:
            user_data = np.array(user_data)

            # Get the range of user_data
            user_data_min = np.min(user_data)
            user_data_max = np.max(user_data)

            # Define intervals over user_data range
            user_intervals = np.linspace(user_data_min, user_data_max, num_intervals + 1)

            # Compute counts and proportions in user_data intervals
            counts, _ = np.histogram(user_data, bins=user_intervals)
            total_counts = counts.sum()
            if total_counts == 0:
                raise ValueError("User data does not contain any valid entries.")

            proportions = counts / total_counts

            # Compute target counts in each interval based on proportions
            target_counts = (proportions * total_sqls).astype(int)

            # Adjust target_counts to ensure they sum up to total_sqls
            count_difference = total_sqls - target_counts.sum()
            if count_difference > 0:
                # Distribute the remaining counts to intervals with the highest proportions
                for _ in range(count_difference):
                    idx = np.argmax(proportions - (target_counts / total_sqls))
                    target_counts[idx] += 1
            elif count_difference < 0:
                # Remove excess counts from intervals with the lowest proportions
                for _ in range(-count_difference):
                    idx = np.argmin(proportions - (target_counts / total_sqls))
                    if target_counts[idx] > 0:
                        target_counts[idx] -= 1

            # Generate SQL costs within each interval in the [min_cost, max_cost] range
            sql_costs = []
            for i in range(num_intervals):
                num_samples = target_counts[i]
                if num_samples > 0:
                    samples = np.random.uniform(intervals[i], intervals[i + 1], num_samples)
                    sql_costs.extend(samples)
            sql_costs = np.array(sql_costs)

            # Handle any discrepancies in total_sqls due to rounding
            if len(sql_costs) < total_sqls:
                remaining = total_sqls - len(sql_costs)
                sql_costs = np.append(sql_costs, np.random.uniform(min_cost, max_cost, remaining))
            elif len(sql_costs) > total_sqls:
                sql_costs = sql_costs[:total_sqls]
        else:
            if distribution == 'normal':
                mean = (min_cost + max_cost) / 2
                stddev = (max_cost - min_cost) / 6
                sql_costs = np.random.normal(loc=mean, scale=stddev, size=total_sqls)
                sql_costs = np.clip(sql_costs, min_cost, max_cost)
            elif distribution == 'uniform':
                sql_costs = np.random.uniform(low=min_cost, high=max_cost, size=total_sqls)
                sql_costs = np.clip(sql_costs, min_cost, max_cost)
            elif distribution == 'exponential':
                raw_data = np.random.exponential(scale=1.0, size=total_sqls)
                sql_costs = min_cost + (raw_data / raw_data.max()) * (max_cost - min_cost)
                sql_costs = np.clip(sql_costs, min_cost, max_cost)
            else:
                assert interval_counts is not None

        # Calculate counts in each interval for the generated SQL costs
        if interval_counts is not None:
            counts = interval_counts
        else:
            counts, _ = np.histogram(sql_costs, bins=intervals)

        self.target_distribution = counts
        self.log(f"Generated target sql distribution: {counts}")

    def distribution_to_midpoint_samples(self, distribution, intervals):
        """
        Convert a distribution of counts in each interval into the corresponding
        list of midpoint samples. If the distribution sum is zero, return a single [0].
        """
        samples = []
        for i, count in enumerate(distribution):
            midpoint = (intervals[i] + intervals[i+1]) / 2
            samples.extend([midpoint] * count)

        # Edge case: if there are no samples in total, add one 0
        if len(samples) == 0:
            samples.append(0)
        return samples

    def compare_and_plot_distributions(self, name):
        """
        Compute Wasserstein distance between two distributions and plot them with different colors.

        Returns:
            float: Wasserstein distance between the two datasets
        """
        target_distribution = self.target_distribution
        current_distribution = self.current_distribution
        min_cost = self.min_cost
        max_cost = self.max_cost

        # Ensure both distributions have the same number of intervals
        assert len(target_distribution) == len(current_distribution), "Both distributions must have the same number of intervals."

        # Modify current_distribution to ensure its values do not exceed the corresponding target_distribution values
        updated_current_distribution = [min(current, target) for current, target in zip(current_distribution, target_distribution)]

        # Number of intervals
        num_intervals = len(target_distribution)
        intervals = np.linspace(min_cost, max_cost, num_intervals + 1)

        # Calculate the Wasserstein distance between the two distributions
        # Convert the binned distributions to midpoint samples
        target_samples = self.distribution_to_midpoint_samples(target_distribution, intervals)
        current_samples = self.distribution_to_midpoint_samples(updated_current_distribution, intervals)
        distance = wasserstein_distance(target_samples, current_samples)

        # Plot the two distributions with different colors
        plt.clf()
        plt.figure(figsize=(6, 3))
        plt.bar(range(num_intervals), target_distribution, alpha=0.5, label='Target Distribution', color='blue', edgecolor='black', 
                tick_label=[f'{int(intervals[i])}-{int(intervals[i+1])}' for i in range(num_intervals)])
        
        plt.bar(range(num_intervals), updated_current_distribution, alpha=0.5, label='Current Distribution', color='orange', edgecolor='black')

        # Set plot labels and title
        plt.xticks(rotation=90, ha='center', fontsize=8)  # Adjusted rotation and font size
        plt.title(f'Comparison of Distributions (Wasserstein Distance: {distance:.4f})')
        plt.xlabel('SQL Cost Intervals')
        plt.ylabel('Number of SQL Queries')
        plt.legend(loc='upper right')
        plt.tight_layout()

        # Store the plot
        result_path = f"{self._root}/outputs/intermediate/result_visualization/cost_distribution/{self.task_name}/{name}_histogram.png"
        os.makedirs(os.path.dirname(result_path), exist_ok=True)

        plt.savefig(result_path)
        plt.close()
        
        return distance

    @timing_decorator
    def template_generation(self, prompt_template, semantic_requirements, generate_new=True):
        template_generator = self.template_generator

        if generate_new is True:
            if isinstance(template_generator, NaiveSQLTemplateGenerator):
                for requirement in semantic_requirements:
                    prompt = template_generator.generate_prompt(
                        prompt_template, 
                            num_of_templates=requirement[0], 
                                semantic_requirement=requirement[1])

                    template_generator.generate_sql_template(prompt, requirement[1])
            else:
                target_real_constraint = "redset_cluster_0_warehouse_132_database_7_data.json"

                prompts = template_generator.generate_prompts(target_real_constraint, semantic_requirements)
             
                template_generator.generate_sql_templates(prompts)
                template_generator.check_and_rewrite_templates_parallel()

        self.template_ids, self.templates = template_generator.load_sql_templates()

    @timing_decorator
    def initial_profiling(self, num_profiling, template_ids=None, templates=None):
        if template_ids is None and templates is None:
            template_ids = self.template_ids    
            templates = self.templates

        profiling_result = {}
        for id in range(len(template_ids)):
            template_id = template_ids[id]
            template = templates[id]

            self.log(f"Start initial profiling of {template_id}")
            file_path = f"./SQLBarber/cost_history/{self.target}/{self.task_name}/initial_sampling_{template_id}.json"
            costs = self.read_cost(file_path)
            if costs is None:
                try:
                    predicate_enumerator = PredicateEnumerator(
                        self.task_name, 
                        self.db_controller, 
                        template_id, 
                        template, 
                        target_cost=10, 
                        file_path=self.column_info_path, 
                        target=self.target,
                        cost_type=self.cost_type
                    )

                    costs = predicate_enumerator.analyze_template(num_profiling)
                    profiling_result[template_id] = costs

                    candidate_queries = predicate_enumerator.queries
                    candidate_costs = predicate_enumerator.costs
                    for query, cost in zip(candidate_queries, candidate_costs):
                        if query not in self.queries:
                            self.queries.append(query)
                            self.costs.append(cost)

                except Exception as e:
                    self.log(f"Failed to process {template_id} due to Error: {e}")
                    self.log(traceback.format_exc()) 
                    continue
            else:
                profiling_result[template_id] = costs
            self.log(f"Finish initial profiling of {template_id}")

        return profiling_result

    def update_distribution_profiling(self, profiling_result):
        """
        Update target distribution based on profiling results
        """
        for template_id, costs in profiling_result.items():
            self.update_distribution(costs)

    def update_distribution(self, costs):
        """
        Update target distribution based on the costs.
        Args:
            distribution (list): The distribution to update (e.g., current_distribution).
            costs (list): List of costs to be added to the distribution.
        """
        if costs is None:
            return

        self.log("--------------------------------------------------")
        self.log(f"Target distribution: {self.target_distribution}")
        self.log(f"Distribution before update: {self.current_distribution}")

        intervals = np.linspace(self.min_cost, self.max_cost, self.num_intervals + 1)
        
        # Update current distribution based on costs
        for cost in costs:
            if cost is not None:
                for i in range(self.num_intervals):
                    if intervals[i] <= cost < intervals[i + 1]:
                        self.current_distribution[i] += 1
                        break

        self.log(f"Distribution after update: {self.current_distribution}")
        self.log("--------------------------------------------------")

    def find_best_template_for_interval(self, interval_index, profiling_result):
        """
        Find the template with the highest probability of producing SQLs in the target interval.
        Also returns the probabilities of all templates.
        
        Args:
            interval_index (int): The index of the target interval.
            profiling_result (dict): Profiling results that contain costs for each template.
        
        Returns:
            str: The ID of the best template to use for this interval.
            dict: A dictionary containing the probabilities of all templates for the target interval.
        """
        intervals = np.linspace(self.min_cost, self.max_cost, self.num_intervals + 1)
        interval_start = intervals[interval_index]
        interval_end = intervals[interval_index + 1]

        best_template_id = None
        highest_probability = 0
        template_probabilities = {}
        
        for template_id, costs in profiling_result.items():
            # Calculate the number of costs that fall in the target interval
            count_in_interval = sum(1 for c in costs if interval_start <= c < interval_end)
            total_count = len(costs)
            
            # Calculate the probability of this template producing SQLs in the target interval
            if total_count > 0:
                probability = count_in_interval / total_count
                template_probabilities[template_id] = probability
                if probability > highest_probability:
                    highest_probability = probability
                    best_template_id = template_id
        
        return best_template_id, template_probabilities
    
    def find_templates_for_interval(self, interval_index, profiling_result):
        """
            Find the templates for the specified interval, based on
            the probability of generating queries in that interval.

            Returns a list of (template_id, probability), sorted by probability descending.
        """
        intervals = np.linspace(self.min_cost, self.max_cost, self.num_intervals + 1)
        interval_start = intervals[interval_index]
        interval_end = intervals[interval_index + 1]

        # Calculate probability per template
        template_probabilities = []
        for template_id, costs in profiling_result.items():
            if costs is None or costs == []:
                continue

            all_none = True
            for cost in costs:
                if cost is not None:
                    all_none = False
            if all_none:
                continue

            closeness_score = self.cal_closeness_template_for_interval(costs, interval_start, interval_end)
            template_probabilities.append((template_id, closeness_score))

        # Sort by probability (descending)
        template_probabilities.sort(key=lambda x: x[1], reverse=True)
        return template_probabilities
    
    def has_limited_cost_diversity(self, template_id, profiling_result, target_interval, unique_cost_threshold=3):
        """
        Check if a template only generates very few unique costs (less than threshold)
        and these costs do not fall within the target interval.
        
        Args:
            template_id: The ID of the template to check
            profiling_result: Dictionary mapping template_ids to their observed costs
            target_interval: [lower_bound, upper_bound] of the target cost interval
            unique_cost_threshold: Maximum number of unique costs to consider "limited diversity"
            
        Returns:
            True if the template has limited cost diversity and no costs in the target interval
        """
        # Get the costs generated by this template
        costs = profiling_result.get(template_id, [])
        
        # Filter out None values
        costs = [c for c in costs if c is not None]
        
        if not costs:
            return False  # No data to judge
        
        # Get unique costs
        unique_costs = set(costs)
        
        # Check if we have very few unique costs
        if len(unique_costs) <= unique_cost_threshold:
            # Check if any of these costs fall within our target interval
            lower_bound, upper_bound = target_interval
            costs_in_range = [c for c in unique_costs if lower_bound <= c < upper_bound]
            
            # If we have few unique costs AND none in the target range, return True
            if not costs_in_range:
                self.log(f"Template {template_id} has only {len(unique_costs)} unique costs and none in target range {target_interval}")
                return True
        
        # Either we have enough diversity or at least some costs in the range
        return False
    
    @timing_decorator
    def optimize_for_interval_naive(self, profiling_result, reuse_history=True):
        """
        Naive version of optimize_for_interval for ablation study.
        - Randomly selects an SQL template
        - Uses initial_profiling() instead of optimize()
        - num_profiling equals the number of optimization iterations in optimize()
        """
        
        while True:
            # Step 1: Find the interval with the largest difference
            interval_index, num_difference = self.find_largest_difference_interval()           
            if interval_index is None or num_difference <= 0: 
                self.log("No more intervals to optimize.")
                return 0

            # Step 2: Get the bounds of the target interval
            intervals = np.linspace(self.min_cost, self.max_cost, self.num_intervals + 1)
            interval_lower_bound = intervals[interval_index]
            interval_upper_bound = intervals[interval_index + 1]
            target_interval = [interval_lower_bound, interval_upper_bound]

            old_diff_in_interval = self.target_distribution[interval_index] - self.current_distribution[interval_index]

            # Step 3: NAIVE - Randomly select a template from all available templates
            available_template_ids = list(profiling_result.keys())
            
            # Filter out templates that have no valid costs
            valid_template_ids = []
            for template_id in available_template_ids:
                costs = profiling_result.get(template_id, [])
                if costs and any(c is not None for c in costs):
                    valid_template_ids.append(template_id)
            
            if not valid_template_ids:
                self.log(f"No valid templates available for interval {interval_index}. Marking as missing.")
                self.missing_intervals.append(interval_index)
                continue
            
            # Randomly select one template
            selected_template_id = random.choice(valid_template_ids)
            
            self.log(f"NAIVE: Randomly selected template {selected_template_id} for interval {interval_index}")
            
            # Step 4: Get the template
            try:
                template_index = self.template_ids.index(selected_template_id)
                template = self.templates[template_index]
            except ValueError:
                self.log(f"Could not find template {selected_template_id} in self.template_ids.")
                continue

            # Step 5: NAIVE - Use initial_profiling instead of optimize
            # Calculate num_profiling based on what optimize() would have used
            num_profiling_naive = int(5 * num_difference)
            
            self.log(f"NAIVE: Using initial_profiling with num_profiling={num_profiling_naive}")
            
            # Create a unique identifier for this profiling run
            file_identifier = f"{self.task_name}_{selected_template_id}_{self.target}_{interval_lower_bound}_to_{interval_upper_bound}_naive"
            
            # Instantiate PredicateEnumerator
            predicate_enumerator = PredicateEnumerator(
                self.task_name, 
                self.db_controller, 
                selected_template_id, 
                template, 
                target_cost=target_interval,
                file_path=self.column_info_path,
                target=self.target,
                cost_type=self.cost_type
            )

            # Use analyze_template (which is what initial_profiling calls internally)
            try:
                new_costs = predicate_enumerator.analyze_template(num_profiling_naive)
                
                # Convert costs if necessary (matching the original logic)
                costs = []
                for cost in new_costs:
                    costs.append(self.calculate_cost(cost))
                new_costs = costs

                # Collect queries and costs for tracking
                candidate_queries = predicate_enumerator.queries
                candidate_costs = predicate_enumerator.costs
                for query, cost in zip(candidate_queries, candidate_costs):
                    if query not in self.queries:
                        self.queries.append(query)
                        self.costs.append(cost)

            except Exception as e:
                self.log(f"Failed to profile template {selected_template_id} due to Error: {e}")
                self.log(traceback.format_exc())
                new_costs = []

            # Step 6: Update current_distribution
            if new_costs:
                self.log("NAIVE: Update current distribution successfully")
                self.update_distribution(new_costs)
                # Update the profiling_result on the fly
                profiling_result[selected_template_id] = profiling_result.get(selected_template_id, []) + new_costs
            else:
                self.log("NAIVE: Profiling result is None or empty")

            # Step 7: Check if there was improvement
            new_diff_in_interval = (
                self.target_distribution[interval_index] - self.current_distribution[interval_index]
            )
            
            if new_diff_in_interval < old_diff_in_interval:
                self.log(f"NAIVE: Improvement found with randomly selected template {selected_template_id}")
            else:
                self.log(f"NAIVE: No improvement with randomly selected template {selected_template_id}")
                # Increment the counter for this interval
                self.selected_times_of_intervals[interval_index] += 1
                
                if self.selected_times_of_intervals[interval_index] >= 5:
                    self.log(f"NAIVE: Interval {interval_index} failed 5 times. Marking as missing.")
                    self.missing_intervals.append(interval_index)
            
            # Return the largest number of difference in an interval
            return num_difference

    @timing_decorator
    def optimize_for_interval(self, profiling_result, reuse_history=True):
        """
            Improved version:
            - Finds the interval with the largest difference.
            - Retrieves the top-k templates (by probability of landing in that interval).
            - Tries each template in descending probability order, skipping "bad" combos
                that gave no improvement in a previous iteration.
            - If none of the top-k yield improvement, mark this interval as missing.
            - Only add (interval_index, template_id) to bad_combinations if:
                fewer than 10% of the new queries helped fill *any* underfilled intervals in current_distribution (not just the target interval).
        """

        while True:
            # Step 1: Find the interval with the largest difference
            interval_index, num_difference = self.find_largest_difference_interval()           
            if interval_index is None or num_difference <= 0: 
                self.log("No more intervals with suitable templates to optimize.")
                return 0

            # Step 2: Get the bounds of the target interval
            intervals = np.linspace(self.min_cost, self.max_cost, self.num_intervals + 1)
            interval_lower_bound = intervals[interval_index]
            interval_upper_bound = intervals[interval_index + 1]
            target_interval = [interval_lower_bound, interval_upper_bound]

            old_diff_in_interval = self.target_distribution[interval_index] - self.current_distribution[interval_index]

            # Step 3: Find the top-k template for the target interval
            # best_template_id, template_probabilities = self.find_best_template_for_interval(interval_index, profiling_result)
            top_k_templates = self.find_templates_for_interval(interval_index, profiling_result)

            # Template filter based on bad_combinations and remaining space size
            filtered_templates = []
            for (template_id, prob) in top_k_templates:
                # Check if this template is in bad_combinations
                if (interval_index, template_id) in self.bad_combinations:
                    continue

                # Check the "remaining_space_size" constraint:
                # If we do not yet have a recorded space size, assume it's infinite
                current_space = self.template_remaining_spaces.get(template_id, float('inf'))
                if current_space < 5 * num_difference:
                    continue

                # Check if template has limited cost diversity outside target range
                if self.has_limited_cost_diversity(template_id, profiling_result, target_interval):
                    self.log(f"Skipping template {template_id} due to limited cost diversity outside the target range")
                    continue
                    
                filtered_templates.append((template_id, prob))

            # Use only the top-10 templates with max probability from the filtered list
            # if len(filtered_templates) >= 10:
            #     filtered_templates = filtered_templates[:10]
            
            # Use probability based sampling
            if len(filtered_templates) > 10:
                # Extract populations and weights
                population = [tpl[0] for tpl in filtered_templates]
                weights    = [tpl[1] for tpl in filtered_templates]
   
                # Sample 10 items in proportion to their probability
                if sum(weights) == 0:
                    # Fall back to uniform random sampling
                    sampled_template_ids = random.sample(population, k=min(10, len(population)))
                else:
                    # Sample 10 items in proportion to their probability
                    sampled_template_ids = random.choices(
                        population=population,
                        weights=weights,
                        k=10
                    )

                # If you need (template_id, probability) pairs from what you sampled:
                # You can rebuild them by matching IDs back to their probabilities.
                # For example:
                sampled_templates = []
                for template_id in sampled_template_ids:
                    # Find probability in filtered_templates again
                    # (in practice, store them in a dictionary for quick lookup)
                    for tpl_id, tpl_prob in filtered_templates:
                        if tpl_id == template_id:
                            sampled_templates.append((tpl_id, tpl_prob))
                            break
                filtered_templates = sampled_templates

            # Step 4: If no suitable template is found, log and try the next interval
            if not filtered_templates:
                self.log(f"No suitable template found for interval {interval_index}. Trying the next interval.")
                self.missing_intervals.append(interval_index)
                continue  # Continue searching for the next interval

            # Step 5: Optimize the interval with top-k templates
            improvement_found = False
            for template_id, prob in filtered_templates:
                if (interval_index, template_id) in self.bad_combinations:
                    continue

                # Copy the distribution before we add these new queries (for counting "useful" queries)
                distribution_before_update = self.current_distribution[:]

                self.log(f"Attempting to optimize interval {interval_index} using template {template_id} (prob={prob:.4f}).")
                template = self.templates[self.template_ids.index(template_id)]

                # Instantiate PredicateEnumerator with the interval as the target_cost
                predicate_enumerator = PredicateEnumerator(
                    self.task_name, 
                    self.db_controller, 
                    template_id, 
                    template, 
                    target_cost=target_interval,  # Pass the interval here
                    file_path=self.column_info_path,
                    target=self.target,
                    cost_type=self.cost_type
                )

                # Optimize for the interval
                new_costs, remaining_space_size = predicate_enumerator.optimize(
                    f"{self.task_name}_{template_id}_{self.target}_{interval_lower_bound}_to_{interval_upper_bound}",
                    trials_number=int(5 * num_difference), 
                    initial_config_number=int(0.5 * num_difference), 
                    reuse_history=reuse_history
                )
                self.template_remaining_spaces[template_id] = remaining_space_size
                costs = []
                for cost in new_costs:
                    costs.append(self.calculate_cost(cost))
                new_costs = costs

                candidate_queries = predicate_enumerator.queries
                candidate_costs = predicate_enumerator.costs
                for query, cost in zip(candidate_queries, candidate_costs):
                    if query not in self.queries:
                        self.queries.append(query)
                        self.costs.append(cost)

                # Step 6: Update current_distribution
                if new_costs != []:
                    self.log("Update current distribution successfully")
                    self.update_distribution(new_costs)
                    # update the profiling_result on the fly
                    profiling_result[template_id] = profiling_result.get(template_id, []) + new_costs
                else:
                    self.log("Optimization result in Func optimize_for_interval is None")

                new_diff_in_interval = (
                    self.target_distribution[interval_index] - self.current_distribution[interval_index]
                )
                if new_diff_in_interval < old_diff_in_interval:
                    self.log(f"Improvement found for template {template_id} in interval {interval_index}.")
                    improvement_found = True

                # Step 7: add bad combination
                total_new = len(new_costs)
                useful_count = self._count_useful_queries(distribution_before_update, new_costs)
                ratio_useful = useful_count / total_new if total_new else 0.0

                # If fewer than 5% of new queries helped fill *any* underfilled intervals => mark as bad
                if ratio_useful < 0.05:
                    self.log(
                        f"No improvement and only {ratio_useful:.2%} of new queries were useful; "
                        "marking as bad combination."
                    )
                    self.bad_combinations.add((interval_index, template_id))

            if not improvement_found:
                # Could not improve with any of the top-10 templates
                self.selected_times_of_intervals[interval_index] += 1
                self.log(f"No improvement found for interval {interval_index} using top-5 templates. Times: {self.selected_times_of_intervals[interval_index]}.")
                if self.selected_times_of_intervals[interval_index] >= 5:
                    self.log(f"No improvement found for interval {interval_index} for {self.selected_times_of_intervals[interval_index]} times. Mark this interval as missing.")
                    self.missing_intervals.append(interval_index)
                
            # Successfully optimized one interval, exit the loop, return the largest number of difference in an interval
            return num_difference
        
    def _count_useful_queries(self, old_distribution, new_costs):
        """
        Count how many of the new queries actually filled an interval
        that was still below the target distribution.

        - We do this by simulating the addition of queries to a copy of old_distribution.
        - Once an interval matches or exceeds its target requirement, further queries in
        that interval are not considered "useful."
        """
        intervals = np.linspace(self.min_cost, self.max_cost, self.num_intervals + 1)
        dist_copy = old_distribution[:]
        useful_count = 0

        for cost in new_costs:
            if cost is not None:
                # Determine which interval this cost belongs to
                for i in range(self.num_intervals):
                    if intervals[i] <= cost < intervals[i + 1]:
                        # If we still need queries in interval i, it's useful
                        if dist_copy[i] < self.target_distribution[i]:
                            useful_count += 1
                            dist_copy[i] += 1  # Mark one more query allocated
                        break

        return useful_count

    def calculate_cost(self, costs):
        cost_type = self.cost_type

        if costs == []:
            return None

        if cost_type == "output_cost":
            return costs[0]   
        elif cost_type == "sum_cost":
            return sum(costs)     

    def read_cost(self, file_path):
        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                data = json.load(file)

            costs = []
            for key, values in data.items():
                costs.append(self.calculate_cost(values))  # final cost

            return costs
        else:
            return None

    def find_largest_difference_interval(self):
        """
        Find the interval with the largest difference between target and current distributions, 
        excluding intervals that have already been found to have no corresponding template.
        
        Returns:
            int: The index of the interval with the largest difference.
            int: The difference in SQL counts for that interval.
        """
        differences = [self.target_distribution[i] - self.current_distribution[i] for i in range(self.num_intervals)]
        
        # Exclude intervals that had no templates found before
        for i in self.missing_intervals:
            differences[i] = float('-inf')  # Set the difference to a very small value to ensure it won't be selected
        
        # Find the interval with the largest difference
        max_diff = max(differences)
        
        if max_diff == float('-inf'):
            return 0, float('-inf')  # If no interval can be optimized, return None

        interval_index = differences.index(max_diff)
        
        # Log the differences for all intervals
        self.log(f"{'Interval':<10}{'Target':<10}{'Current':<10}{'Difference':<10}")
        for i in range(self.num_intervals):
            self.log(f"{i:<10}{self.target_distribution[i]:<10}{self.current_distribution[i]:<10}{differences[i]:<10}")
        
        return interval_index, max_diff

    def cal_closeness_template_for_interval(self, costs, interval_start, interval_end):
        """
            Helper function to score how well a template covers a specific interval
            Compute how 'close' a template's costs are to a given interval,
            then scale by how many distinct costs it can produce 
            (templates with very few distinct costs get penalized).
        """
        if not costs:
            return 0.0

        all_none = True
        clean_costs = []
        for cost in costs:
            if cost is not None:
                all_none = False
                clean_costs.append(cost)

        if all_none:
            return 0.0
        costs = clean_costs

        # 1) Calculate the total "distance" from the interval.
        total_distance = 0.0
        for c in costs:
            if c < interval_start:
                total_distance += (interval_start - c)
            elif c > interval_end:
                total_distance += (c - interval_end)
            else:
                total_distance += 0  # cost is within the interval

        # 2) Base closeness score
        avg_distance = total_distance / len(costs)
        base_score = 1.0 / (1.0 + avg_distance)  # smaller distance => bigger score

        # 3) Variety factor: ratio of distinct cost values to total cost values
        distinct_count = len(set(costs))
        variety_factor = distinct_count / len(costs)

        # 4) Final score = closeness * variety
        final_score = base_score * variety_factor
        
        return final_score

    # no use now
    def generate_direct_templates_for_interval(self, interval_idx, num_templates=20):
        """
        Generate SQL templates directly targeting a specific cost interval without predefined constraints.
        The only constraint is the target cost range.
        
        Args:
            interval_idx: The index of the interval to target
            num_templates: Number of templates to generate (default: 20)
            
        Returns:
            List of SQL templates, list of template IDs
        """
        self.log(f"Generating unconstrained direct templates for interval {interval_idx}")
        
        # Get the cost range for this interval
        intervals = np.linspace(self.min_cost, self.max_cost, self.num_intervals + 1)
        interval_start = intervals[interval_idx]
        interval_end = intervals[interval_idx + 1]
        target_cost_range = (interval_start, interval_end)
        
        # Get the template generator
        template_generator = self.template_generator
        
        # Get the database schema
        db_schema = template_generator.db_schema
        
        # Create a list to store template IDs and prompts
        template_ids = []
        prompts = []
        
        # Get the next available template ID
        next_id = self._get_next_template_id()
        
        # Generate multiple templates with different prompts
        for i in range(num_templates):
            template_id = next_id + i
            template_ids.append(template_id)
            
            # Build a detailed prompt focused on the database schema and cost range
            prompt = f"""
    Generate an SQL template with placeholders for predicate values that will have a cost in the range [{interval_start}, {interval_end}].

    Database Schema Information:
    """
            # Include full database schema details
            for table_name, table_info in db_schema['tables'].items():
                prompt += "-" * 80 + "\n"
                # Table info line
                prompt += f"Table: {table_name}, Size: {table_info['size']}, Row Count: {table_info['row_count']}\n"
                
                # Column header
                prompt += f"{'Column Name':<30} {'Data Type':<20} {'Unique Values':<15} {'NOT NULL'}\n"
                prompt += "-" * 80 + "\n"

                # Column details
                for column_name, column_info in table_info['columns'].items():
                    data_type = column_info['data_type']
                    unique_values = column_info['unique_values']
                    not_null = "Yes" if not column_info['is_nullable'] else "No"
                    prompt += f"{column_name:<30} {data_type:<20} {str(unique_values):<15} {not_null}\n"

                # Primary Keys
                if table_info.get('primary_keys'):
                    if len(table_info['primary_keys']) > 0:
                        pks = ', '.join(table_info['primary_keys'])
                        prompt += f"Primary Key: {pks}\n"
                    else:
                        prompt += "Primary Key: None\n"
                else:
                    prompt += "Primary Key: None\n"

                # Foreign Keys
                if table_info.get('foreign_keys'):
                    if len(table_info['foreign_keys']) > 0:
                        for fk in table_info['foreign_keys']:
                            prompt += (
                                f"Foreign Key: {fk['column']} -> "
                                f"{fk['references']['table']}({fk['references']['column']})\n"
                            )
                    else:
                        prompt += "Foreign Key: None\n"
                else:
                    prompt += "Foreign Key: None\n"

                # Indexes (important for query cost)
                if table_info.get('indexes'):
                    if len(table_info['indexes']) > 0:
                        prompt += "Indexes:\n"
                        for idx in table_info['indexes']:
                            prompt += f"{idx['name']}: {idx['definition']}\n"
                    else:
                        prompt += "Indexes: None\n"
                else:
                    prompt += "Indexes: None\n"

                prompt += "\n"

            # Add guidance for creating SQL with target cost range
            prompt += f"""
    Target Cost Range: [{interval_start}, {interval_end}]

    You are tasked with creating an SQL template that will have an execution cost within the target range.

    Cost Considerations:
    - Higher costs typically involve: more joins, larger tables, complex operations, fewer filters
    - Lower costs typically involve: fewer joins, smaller tables, simpler operations, selective filters
    - Using indexes can significantly reduce costs
    - Table size and row count are good indicators of potential query cost
    - You can use any table set to satisfy the requirement

    To generate a variety of templates, consider one of these approaches for template #{i+1}:
    - {"Focus on a single large table with complex filters" if i % 5 == 0 else ""}
    - {"Create a query with multiple joins across different tables" if i % 5 == 1 else ""}
    - {"Use aggregation operations (COUNT, SUM, AVG, etc.)" if i % 5 == 2 else ""}
    - {"Include subqueries or nested operations" if i % 5 == 3 else ""}
    - {"Mix different types of operations (joins, aggregations, filters)" if i % 5 == 4 else ""}

    Format Requirement:
    - Predicate values (the dynamic values that will be inserted for filtering) should be wrapped in double curly braces with single quotes like `'{{{{}}}}'`.
    - Ensure that all predicate values wrapped in double curly braces are enclosed in single quotes, e.g., `'{{{{real_table_name.real_column_name}}}}'`.
    - Table names, column names, and JOIN conditions should be written directly without any curly braces or quotes. Double curly braces with single quotes are only for placeholders where predicate values will be inserted.
    - For predicates with both lower and upper bounds, use `'{{{{real_table_name.real_column_name_start}}}}'` and `'{{{{real_table_name.real_column_name_end}}}}'` to represent the placeholder values, but do not wrap the actual column names in curly braces.
    - The table names and column names should exactly match those in the database. Include both real table name and column name like `'{{{{real_table_name.real_column_name_end}}}}'`.

    Now let's think step by step and provide the SQL query template. Return the result in JSON format as:
    {{
        "sql_template": "Your SQL template here",
        "think_process": "Your step by step thinking here"
    }}
    """
            
            # Add a simple constraint to make each template unique
            constraints = {
                'num_tables_accessed': None,
                'num_joins': None, 
                'num_aggregations': None,
                'semantic_requirement': f"Generate a template with cost in range [{interval_start}, {interval_end}]",
                'tables_involved': list(db_schema['tables'].keys())  # All tables available
            }
            
            prompts.append({'template_id': template_id, 'prompt': prompt, 'constraints': constraints})
        
        # Generate the SQL templates with target cost range
        sql_templates = template_generator.generate_sql_templates(prompts, target_cost_range=target_cost_range)
        
        return sql_templates, template_ids

    # no use now
    def _get_next_template_id(self):
        """Helper method to get the next available template ID"""
        # Check existing template files to find the highest ID
        folder_path = self.seed_template_path
        special_folder = os.path.join(folder_path, self.summary_name)
        
        existing_ids = []
        for folder in [folder_path, special_folder]:
            if os.path.exists(folder):
                for filename in os.listdir(folder):
                    if filename.startswith("template_") and filename.endswith(".sql"):
                        match = re.match(r"template_(\d+)\.sql", filename)
                        if match:
                            file_id = int(match.group(1))
                            existing_ids.append(file_id)
        
        return max(existing_ids) + 1 if existing_ids else 1

    @timing_decorator
    def template_refinement_parallel(self, profiling_result, num_profiling):
        """
        We refine existing templates to cover intervals that produced zero (or very few) queries 
        in the initial profiling stage with parallelized processing for better performance.
        
        This enhanced version adds:
        1. Double parallelism: parallelizing both interval processing and template refinement
        2. Additional phase for "difficult intervals" (with <10% of target queries)
        3. Few-shot learning with feedback from previous iterations for difficult intervals
        """
        # ------------------------------------------------------------------------
        # 0) Early exit if using NaiveSQLTemplateGenerator
        # ------------------------------------------------------------------------
        template_generator = self.template_generator
        if isinstance(template_generator, NaiveSQLTemplateGenerator):
            self.log("NaiveSQLTemplateGenerator does not support refinement. Skipping.")
            return profiling_result, None

        # ------------------------------------------------------------------------
        # 1) Compute how many queries each cost-interval received in initial profiling
        # ------------------------------------------------------------------------
        intervals = np.linspace(self.min_cost, self.max_cost, self.num_intervals + 1)
        interval_coverage = [0] * self.num_intervals

        # Gather all costs from the entire initial profiling
        for t_id, costs in profiling_result.items():
            if costs is None:
                continue
            for cost in costs:
                if cost is not None:
                    for i in range(self.num_intervals):
                        if intervals[i] <= cost < intervals[i + 1]:
                            interval_coverage[i] += 1
                            break

        self.log("===== Interval Coverage after Initial Profiling =====")
        for i in range(self.num_intervals):
            self.log(f"Interval {i}: {interval_coverage[i]} queries")

        # ------------------------------------------------------------------------
        # 2) Identify missing and difficult intervals
        # ------------------------------------------------------------------------
        # num_profiling = 0.15 * total_sql
        # threshold_number = 0.2 * num_profiling * len(self.templates) / self.num_intervals
        def get_missing_intervals():
            missing = []
            for i in range(self.num_intervals):
                if interval_coverage[i] == 0:
                    missing.append(i)
                else:
                    threshold_number = self.target_distribution[i] * 0.2
                    if interval_coverage[i] < threshold_number:
                        missing.append(i)
            return missing
        
        def get_difficult_intervals():
            difficult = []
            for i in range(self.num_intervals):
                # No queries at all or less than 10% of target
                threshold_number = self.target_distribution[i] * 0.1
                if interval_coverage[i] == 0 or interval_coverage[i] < threshold_number:
                    difficult.append(i)
            return difficult

        # ------------------------------------------------------------------------
        # 3) Prepare for template ID assignment and tracking history
        # ------------------------------------------------------------------------
        # For few-shot learning, track template history per interval
        # Format: {interval_idx: [(template_string, costs), ...]}
        interval_templates_history = {}
        
        folder_path = self.seed_template_path
        os.makedirs(folder_path, exist_ok=True)

        special_folder = os.path.join(folder_path, self.summary_name)
        os.makedirs(special_folder, exist_ok=True)

        existing_ids = []
        for folder in [folder_path, special_folder]:
            for filename in os.listdir(folder):
                if filename.startswith("template_") and filename.endswith(".sql"):
                    match = re.match(r"template_(\d+)\.sql", filename)
                    if match:
                        file_id = int(match.group(1))
                        existing_ids.append(file_id)
        start_num = max(existing_ids) + 1 if existing_ids else 1
        global_id_counter = 0
        
        # ------------------------------------------------------------------------
        # HELPER FUNCTION: refine templates for a single interval in parallel
        # ------------------------------------------------------------------------
        def _refine_one_interval(i_idx, num_templates=5, use_few_shot=False):
            """
            Return a list of new SQL template strings for the missing interval i_idx.
            Uses parallel processing to refine multiple templates simultaneously.
            
            Args:
                i_idx: The interval index to refine
                num_templates: Number of top templates to refine
                use_few_shot: Whether to use few-shot learning with feedback
            """
            interval_start = intervals[i_idx]
            interval_end = intervals[i_idx + 1]

            self.log(
                f">>> Interval {i_idx} has coverage={interval_coverage[i_idx]} "
                f"(of {self.target_distribution[i_idx]}), "
                f"range=[{interval_start}, {interval_end}). Refining..."
            )

            # Score existing templates for how well they already produce queries in this interval
            template_scores = []
            for t_id, costs in profiling_result.items():
                score = self.cal_closeness_template_for_interval(
                    costs, interval_start, interval_end
                )
                template_scores.append((t_id, score))

            # Sort descending by score
            template_scores.sort(key=lambda x: x[1], reverse=True)

            if len(template_scores) <= 3:
                return []

            population = [tpl[0] for tpl in template_scores]
            weights = [tpl[1] for tpl in template_scores]

            # Sample top templates based on requested amount
            sampled_template_ids = random.choices(
                population=population,
                weights=weights,
                k=num_templates
            )

            # Build the top_templates structure
            top_templates = []
            for template_id in sampled_template_ids:
                for tpl_id, tpl_prob in template_scores:
                    if tpl_id == template_id:
                        top_templates.append((tpl_id, tpl_prob))
                        break

            # Prepare for parallel template refinement
            newly_generated = []
            
            with ThreadPoolExecutor() as executor:
                futures = {}
                
                for (t_id, score) in top_templates:
                    try:
                        template_idx = self.template_ids.index(t_id)
                        old_sql_template = self.templates[template_idx]
                        
                        # Decide whether to use few-shot learning for this interval
                        if use_few_shot and i_idx in interval_templates_history and interval_templates_history[i_idx]:
                            # Get history templates and costs for this interval
                            history_templates = []
                            history_costs = []
                            
                            for template, costs in interval_templates_history[i_idx]:
                                history_templates.append(template)
                                history_costs.append(costs)
                            
                            # Add current template to the mix
                            all_templates = history_templates + [old_sql_template]
                            all_costs = history_costs + [profiling_result.get(t_id, [])]
                            
                            # Submit with few-shot learning
                            future = executor.submit(
                                template_generator.refine_templates,
                                cost_type=self.target,
                                old_sql_templates=all_templates,
                                old_costs_list=all_costs,
                                target_cost_range=(interval_start, interval_end)
                            )
                        else:
                            # Standard refinement (no few-shot)
                            future = executor.submit(
                                template_generator.refine_templates,
                                cost_type=self.target,
                                old_sql_templates=old_sql_template,
                                old_costs_list=profiling_result.get(t_id, []),
                                target_cost_range=(interval_start, interval_end)
                            )
                        
                        futures[future] = t_id
                        
                    except ValueError:
                        self.log(f"Could not find template {t_id} in self.template_ids.")
                        continue
                
                # Collect results as they complete
                for future in as_completed(futures):
                    t_id = futures[future]
                    try:
                        template_results = future.result()
                        newly_generated.extend(template_results)
                    except Exception as e:
                        self.log(f"Error refining template {t_id}: {e}")
                        self.log(traceback.format_exc())

            return newly_generated

        # ------------------------------------------------------------------------
        # HELPER FUNCTION: determine if template should be kept or pruned
        # ------------------------------------------------------------------------
        def template_pruning(costs):
            """
            Helper function to determine if a template should be kept or pruned.
            Returns True if template should be pruned, False if it should be kept.
            
            1. Fill any missing intervals
            2. Keep if it can decrease distribution distance
            """
            if costs is None:
                return True
                
            coverage_counts = [0] * self.num_intervals
            for c in costs:
                if c is not None:
                    for idx in range(self.num_intervals):
                        if intervals[idx] <= c < intervals[idx + 1]:
                            coverage_counts[idx] += 1

            # Check missing intervals
            for mi_idx in get_missing_intervals():
                if coverage_counts[mi_idx] > 0:
                    return False  # Keep this template

            # Check if it can decrease the distance
            for i in range(self.num_intervals):
                difference = self.target_distribution[i] - self.current_distribution[i]
                if difference > 0 and coverage_counts[i] > 0:
                    return False  # Keep

            return True  # if we never found a reason to keep it, we prune
        
        # ------------------------------------------------------------------------
        # HELPER FUNCTION: process and save templates 
        # ------------------------------------------------------------------------
        def process_and_save_templates(new_templates_all, phase_name, interval_idx=None):
            """
            Helper function to profile, evaluate, and save new templates.
            If interval_idx is provided, also tracks templates for few-shot learning.
            
            Returns updated profiling_result and minimum distance achieved.
            """
            nonlocal global_id_counter, profiling_result
            
            if not new_templates_all:
                self.log(f"No new templates generated in {phase_name}.")
                return profiling_result, None
                
            self.log(f"In {phase_name}, generated {len(new_templates_all)} new templates. Profiling them...")
                
            # Assign new template IDs
            new_template_ids = []
            for _ in range(len(new_templates_all)):
                new_id_int = start_num + global_id_counter
                global_id_counter += 1
                new_template_ids.append(f"template_{new_id_int}")

            # Profile the new templates
            try:
                newly_profiled = self.initial_profiling(
                    num_profiling=int(0.15 * self.total_sqls),
                    template_ids=new_template_ids,
                    templates=new_templates_all
                )
            except Exception as e:
                self.log(f"Failed to profile newly refined templates. Error: {e}")
                return profiling_result, None

            # Process each template: keep or prune
            accepted_template_ids = []
            accepted_sql_strings = []
            local_distances = []

            # Store template history for the specific interval if provided
            if interval_idx is not None:
                if interval_idx not in interval_templates_history:
                    interval_templates_history[interval_idx] = []

            for nt_id, costs in newly_profiled.items():
                if costs is None:
                    continue

                # Store template and costs for few-shot learning if interval provided
                if interval_idx is not None:
                    template_idx = new_template_ids.index(nt_id)
                    template_str = new_templates_all[template_idx]
                    
                    # Keep at most 3 templates per interval
                    if len(interval_templates_history[interval_idx]) < 3:
                        interval_templates_history[interval_idx].append((template_str, costs))
                    else:
                        # Replace the template with the worst distance
                        worst_idx = -1
                        worst_distance = -1
                        target_range = [intervals[interval_idx], intervals[interval_idx+1]]
                        
                        for i, (_, old_costs) in enumerate(interval_templates_history[interval_idx]):
                            valid_costs = [c for c in old_costs if c is not None]
                            if not valid_costs:
                                worst_idx = i
                                break
                                
                            avg_old_cost = sum(valid_costs) / len(valid_costs)
                            min_target, max_target = target_range
                            
                            if avg_old_cost < min_target:
                                distance = min_target - avg_old_cost
                            elif avg_old_cost > max_target:
                                distance = avg_old_cost - max_target
                            else:
                                distance = 0
                            
                            if distance > worst_distance:
                                worst_distance = distance
                                worst_idx = i
                        
                        # Replace if the new template has better distance
                        valid_new_costs = [c for c in costs if c is not None]
                        if valid_new_costs:
                            avg_new_cost = sum(valid_new_costs) / len(valid_new_costs)
                            min_target, max_target = target_range
                            
                            if avg_new_cost < min_target:
                                new_distance = min_target - avg_new_cost
                            elif avg_new_cost > max_target:
                                new_distance = avg_new_cost - max_target
                            else:
                                new_distance = 0
                            
                            if new_distance < worst_distance or worst_distance == -1:
                                interval_templates_history[interval_idx][worst_idx] = (template_str, costs)

                # Decide to keep or prune
                if not template_pruning(costs):
                    accepted_template_ids.append(nt_id)
                    template_idx = new_template_ids.index(nt_id)
                    accepted_sql_strings.append(new_templates_all[template_idx])
                    
                    # Register to global profiling
                    # parse the numeric ID
                    numeric_id = int(nt_id.split("_")[1])
                    profiling_result[numeric_id] = costs
                    self.update_distribution(costs)
                    distance = self.compare_and_plot_distributions(
                        f"{phase_name}_{numeric_id}"
                    )
                    local_distances.append(distance)

                    # Update coverage
                    for c in costs:
                        if c is not None:
                            for idx in range(self.num_intervals):
                                if intervals[idx] <= c < intervals[idx + 1]:
                                    interval_coverage[idx] += 1
                                    break

            # Write accepted templates to disk
            for idx, nt_id in enumerate(accepted_template_ids):
                numeric_id = int(nt_id.split("_")[1])
                new_filename = f"template_{numeric_id}.sql"
                file_path = os.path.join(special_folder, new_filename)
                formatted_sql = sqlparse.format(accepted_sql_strings[idx], 
                                            reindent=True, 
                                            keyword_case="upper")
                with open(file_path, 'w') as f:
                    f.write(formatted_sql)

                # Also add to our local in-memory
                self.template_ids.append(numeric_id)
                self.templates.append(accepted_sql_strings[idx])

            self.log(f"In {phase_name}, accepted {len(accepted_template_ids)} new template(s)")
            self.log(f"===== Interval Coverage after {phase_name} =====")
            for i in range(self.num_intervals):
                self.log(f"Interval {i}: {interval_coverage[i]} queries")
                
            return profiling_result, min(local_distances) if local_distances else None

        # ------------------------------------------------------------------------
        # 4) Main Refinement Loop (original 5 iterations)
        # ------------------------------------------------------------------------
        distances = []
        max_refine_num = 3
        
        for refine_iter in range(max_refine_num):
            self.log(f"=== Main Template Refinement Iteration {refine_iter+1}/{max_refine_num} ===")
            missing_intervals = get_missing_intervals()
            if not missing_intervals:
                self.log("No missing intervals left. Main refinement complete.")
                break

            # Parallel: refine each missing interval in threads
            new_templates_all = []
            interval_templates_map = {}  # Track which new templates came from which interval
            
            with ThreadPoolExecutor() as executor:
                futures = {}
                for i_idx in missing_intervals:
                    future = executor.submit(_refine_one_interval, i_idx, 3)
                    futures[future] = i_idx

                for future in as_completed(futures):
                    i_idx = futures[future]
                    try:
                        result_templates = future.result()
                        start_idx = len(new_templates_all)
                        new_templates_all.extend(result_templates)
                        # Track which templates came from which interval
                        for i in range(start_idx, len(new_templates_all)):
                            interval_templates_map[i] = i_idx
                    except Exception as exc:
                        self.log(f"Interval {i_idx} encountered an error: {exc}")

            # Process and save the templates with interval tracking for few-shot learning
            phase_name = f"main_refine_{refine_iter+1}"
            
            # Since we need to track templates per interval, process each template individually
            for i, template in enumerate(new_templates_all):
                interval_idx = interval_templates_map.get(i)
                if interval_idx is not None:
                    single_result, distance = process_and_save_templates([template], f"{phase_name}_{i}", interval_idx)
                    if distance is not None:
                        distances.append(distance)
            
        # ------------------------------------------------------------------------
        # 5) Additional Refinement for Difficult Intervals (3 more iterations)
        # ------------------------------------------------------------------------
        max_difficult_refine_num = 5
        
        for refine_iter in range(max_difficult_refine_num):
            difficult_intervals = get_difficult_intervals()
            
            if not difficult_intervals:
                self.log("No difficult intervals left. Additional refinement complete.")
                break
                
            self.log(f"=== Additional Refinement for Difficult Intervals: Iteration {refine_iter+1}/{max_difficult_refine_num} ===")
            self.log(f"Difficult intervals identified: {difficult_intervals}")
            
            # Parallel: refine each difficult interval with more templates (10 instead of 5)
            # and use few-shot learning from previous iterations
            new_templates_all = []
            interval_templates_map = {}  # Track which new templates came from which interval
            
            with ThreadPoolExecutor() as executor:
                futures = {}
                for i_idx in difficult_intervals:
                    # Use 10 templates for difficult intervals and enable few-shot learning after first iteration
                    future = executor.submit(_refine_one_interval, i_idx, 5, refine_iter > 0)
                    futures[future] = i_idx

                for future in as_completed(futures):
                    i_idx = futures[future]
                    try:
                        result_templates = future.result()
                        start_idx = len(new_templates_all)
                        new_templates_all.extend(result_templates)
                        # Track which templates came from which interval
                        for i in range(start_idx, len(new_templates_all)):
                            interval_templates_map[i] = i_idx
                    except Exception as exc:
                        self.log(f"Difficult interval {i_idx} encountered an error: {exc}")
            
            # Process and save the templates with interval tracking
            phase_name = f"difficult_refine_{refine_iter+1}"
            
            # Process each template individually to track by interval
            for i, template in enumerate(new_templates_all):
                interval_idx = interval_templates_map.get(i)
                if interval_idx is not None:
                    single_result, distance = process_and_save_templates([template], f"{phase_name}_{i}", interval_idx)
                    if distance is not None:
                        distances.append(distance)
                
        self.log("Template refinement process is complete.")
        return profiling_result, min(distances) if distances else None
    
    def save_workload_and_summary(self, distances, timestamps, start_time, end_time):
        """
        Save workload and summary information in JSON formats.
        """
        
        # First, create the workload.json
        workload_data = []
        valid_query_count = 0
        
        for idx, (query, cost) in enumerate(zip(self.queries, self.costs)):
            # Skip queries with None cost or outside the cost range
            if cost is None or cost < self.min_cost or cost > self.max_cost:
                continue
            
            valid_query_count += 1
            
            # Extract template ID from the query metadata
            template_id_match = re.search(r'-- Template ID: (\d+)', query)
            template_id = int(template_id_match.group(1)) if template_id_match else None
            
            # Extract just the SQL query (remove metadata comments)
            query_lines = query.split('\n')
            sql_start_idx = 0
            for i, line in enumerate(query_lines):
                if not line.strip().startswith('--') and line.strip():
                    sql_start_idx = i
                    break
            
            actual_query = '\n'.join(query_lines[sql_start_idx:]).strip()
            
            workload_data.append({
                'query_id': valid_query_count,
                'template_id': template_id,
                'query': actual_query,
                'cost_type': self.target,
                'cost': cost
            })
        
        # Sort by cost
        workload_data.sort(key=lambda x: x['cost'])
        
        # Update query_ids after sorting
        for idx, item in enumerate(workload_data):
            item['query_id'] = idx + 1
        
        # Save workload.json
        workload_file = self.workload_file
        with open(workload_file, 'w') as f:
            json.dump(workload_data, f, indent=2)
        
        # Now create the summary.json
        
        # Calculate template statistics
        template_stats = defaultdict(int)
        for item in workload_data:
            if item['template_id'] is not None:
                template_stats[item['template_id']] += 1
        
        # Calculate cost interval distribution
        intervals = np.linspace(self.min_cost, self.max_cost, self.num_intervals + 1)
        actual_distribution = [0] * self.num_intervals
        
        for item in workload_data:
            cost = item['cost']
            # Find which interval this cost belongs to
            for i in range(self.num_intervals):
                if intervals[i] <= cost < intervals[i + 1]:
                    actual_distribution[i] += 1
                    break
                elif i == self.num_intervals - 1 and cost == intervals[i + 1]:
                    # Handle edge case where cost equals max_cost
                    actual_distribution[i] += 1
                    break
        
        # Create cost interval bounds for clarity
        interval_bounds = []
        for i in range(self.num_intervals):
            interval_bounds.append({
                'interval_id': i + 1,
                'lower_bound': float(intervals[i]),
                'upper_bound': float(intervals[i + 1]),
                'target_count': self.target_distribution[i] if self.target_distribution else 0,
                'actual_count': actual_distribution[i]
            })
        
        summary_data = {
            'task_name': self.task_name,
            'generation_parameters': {
                'total_sqls_requested': self.total_sqls,
                'total_sqls_generated': len(workload_data),
                'min_cost': self.min_cost,
                'max_cost': self.max_cost,
                'num_intervals': self.num_intervals,
                'target_type': self.target,
                'cost_type': self.cost_type
            },
            'performance_metrics': {
                'target_distribution': self.target_distribution,
                'actual_distribution(down-sampling to get exact number)': actual_distribution,
                'distances': distances,
                'timestamps': timestamps,
                'total_time_minutes': (end_time - start_time) / 60
            },
            'llm_usage': {
                'model': self.gpt.model,
                'prompt_tokens': self.gpt.total_prompt_tokens,
                'completion_tokens': self.gpt.total_completion_tokens,
                'total_cost_dollars': self.gpt.total_dollars
            },
            'template_statistics': {
                'total_templates_used': len(template_stats),
                'queries_per_template': dict(sorted(template_stats.items())),
                'total_templates_generated': len(self.templates) if self.templates else 0
            },
            'cost_interval_details': interval_bounds
        }
        
        # Save summary.json
        summary_file = self.summary_file
        with open(summary_file, 'w') as f:
            json.dump(summary_data, f, indent=2)
        
        print(f"Workload saved to: {workload_file}")
        print(f"Summary saved to: {summary_file}")

    @timing_decorator
    def generate_sql(self, prompt_template, semantic_requirements, num_iterations=10, num_profiling=200, generate_new_sql_tamplate=True, reuse_history=True):
        """
        Generate SQL queries to match the target distribution, optimizing based on profiling results.
        
        Args:
            prompt_template (str): The SQL prompt template.
            semantic_requirements (list): List of semantic requirements for the SQL generation.
            num_iterations (int): Number of iterations to optimize the current distribution.
        """

        timestamps = []
        distances = []
        distance = self.compare_and_plot_distributions("target_distribution")
        distances.append(distance)
        start_time = time.time()
        timestamps.append(start_time)
        
        # Step 1: Generate SQL templates
        self.template_generation(prompt_template, semantic_requirements, generate_new_sql_tamplate)
        
        # Step 2: Initial profiling of templates
        profiling_result = self.initial_profiling(num_profiling)
        self.update_distribution_profiling(profiling_result)
        distance = self.compare_and_plot_distributions("initial_profiling")
        distances.append(distance)
        timestamps.append(time.time())

        # Step 3: Refine templates
        profiling_result, distance = self.template_refinement_parallel(profiling_result, num_profiling)
        distances.append(distance)
        timestamps.append(time.time())

        # Step 4: Re-Initialize a list to track missing intervals
        self.missing_intervals = []
        # Step 5: Iteratively optimize until the current distribution matches the target distribution
        for iteration in range(num_iterations):
            self.log(f"Iteration {iteration + 1}/{num_iterations}")

            # Step 6: Optimize for the interval with the largest difference
            # num_difference = self.optimize_for_interval_naive(profiling_result, reuse_history=reuse_history)
            num_difference = self.optimize_for_interval(profiling_result, reuse_history=reuse_history)

            # Step 7: Optionally, plot the current vs. target distribution for monitoring
            distance = self.compare_and_plot_distributions(f"iteration_{iteration + 1}")
            distances.append(distance)
            timestamps.append(time.time())
            self.log(f"The wasserstein_distance after iteration {iteration + 1} is {distance}")

            # no difference between current distribution and target distribution
            if num_difference <= 0:
                self.log("Target distribution is matched. Stopping optimization.")
                break

            # Stopping criteria 1: Check if total time exceeds 1 hour (3600 seconds)
            elapsed_time = time.time() - start_time
            if elapsed_time > 3600:
                self.log(f"Stopping optimization: elapsed time ({elapsed_time:.2f}s) exceeded 1 hour.")
                break

            # Stopping criteria 2: Check if distance hasn't changed for the last 3 iterations
            # We need at least 3 distance values after the current iteration (excluding initial profiling distances)
            # distances has: [target_distribution, initial_profiling, refinement, iteration_1, iteration_2, ...]
            # So iteration distances start from index 3
            iteration_distances = distances[3:]  # Get only iteration distances
            if len(iteration_distances) >= 3:
                # Check the last 5 distances
                last_three = iteration_distances[-3:]
                if len(set(last_three)) == 1:  # All three distances are the same
                    self.log(f"Stopping optimization: distance has not changed for the last 3 iterations (distance={last_three[0]}).")
                    break
        end_time = time.time()

        # Step 8: Log the missing intervals for which no templates were found
        if self.missing_intervals:
            self.log(f"Intervals with no corresponding templates: {self.missing_intervals}")

        # Step 9: output the generated SQL workload and summay of the generation
        self.save_workload_and_summary(distances, timestamps, start_time, end_time)