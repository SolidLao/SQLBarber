"""
CPU Cost Calculator for PostgreSQL Queries
Based on PostgreSQL's internal CPU cost estimation formulas
"""

import re
import math
import json
from typing import Any, Dict, List, Tuple, Optional


class CPUCostCalculator:
    """
    Mirrors PostgreSQL CPU-only cost equations on top of EXPLAIN (FORMAT JSON) plans.
    """

    # Regex pattern to identify operators in SQL expressions
    OP_TOKENS = re.compile(
        r"\b(=|<>|<=|>=|<|>|LIKE|ILIKE|BETWEEN|IS\s+NOT|IS\s+NULL|IN\s*\(|@>|<@|&&)\b",
        re.I
    )

    def __init__(self, db_controller):
        """
        Initialize the CPU cost calculator.

        Args:
            db_controller: Database controller instance for executing queries
        """
        self.db_controller = db_controller
        self.gucs = self._get_gucs()

    def _get_gucs(self) -> Dict[str, float]:
        """
        Retrieve PostgreSQL GUC (Grand Unified Configuration) cost parameters.

        Returns:
            Dictionary of cost parameters
        """
        gucs = {}
        guc_names = [
            "cpu_tuple_cost",
            "cpu_index_tuple_cost",
            "cpu_operator_cost",
            "parallel_setup_cost",
            "parallel_tuple_cost"
        ]

        for g in guc_names:
            try:
                result = self.db_controller.execute_sql(f"SHOW {g}")
                if result["error"] is None and result["result"]:
                    gucs[g] = float(result["result"][0][0])
                else:
                    # Default values from PostgreSQL
                    defaults = {
                        "cpu_tuple_cost": 0.01,
                        "cpu_index_tuple_cost": 0.005,
                        "cpu_operator_cost": 0.0025,
                        "parallel_setup_cost": 1000.0,
                        "parallel_tuple_cost": 0.1
                    }
                    gucs[g] = defaults.get(g, 0.01)
            except Exception as e:
                # Use default values if query fails
                defaults = {
                    "cpu_tuple_cost": 0.01,
                    "cpu_index_tuple_cost": 0.005,
                    "cpu_operator_cost": 0.0025,
                    "parallel_setup_cost": 1000.0,
                    "parallel_tuple_cost": 0.1
                }
                gucs[g] = defaults.get(g, 0.01)

        return gucs

    def explain_json(self, sql: str) -> Optional[Dict[str, Any]]:
        """
        Execute EXPLAIN (FORMAT JSON) on a SQL query.

        Args:
            sql: SQL query string

        Returns:
            JSON plan dictionary or None if error
        """
        try:
            result = self.db_controller.execute_sql(f"EXPLAIN (FORMAT JSON) {sql}")
            if result["error"] is None and result["result"]:
                plan_json = result["result"][0][0]
                # Handle both string and dict responses
                if isinstance(plan_json, str):
                    plan_json = json.loads(plan_json)
                if isinstance(plan_json, list) and len(plan_json) > 0:
                    return plan_json[0]
                return plan_json
        except Exception as e:
            print(f"Error executing EXPLAIN JSON: {e}")
        return None

    @staticmethod
    def count_ops(expr: Optional[str]) -> int:
        """
        Count operators in a SQL expression.

        Args:
            expr: SQL expression string

        Returns:
            Number of operators found (minimum 1 if expression exists)
        """
        if not expr:
            return 0
        return max(1, len(CPUCostCalculator.OP_TOKENS.findall(expr)))

    @staticmethod
    def log2_safe(n: float) -> float:
        """Safe logarithm base 2 with minimum value of 2.0"""
        return math.log2(max(2.0, n))

    @staticmethod
    def node_rows(node: Dict[str, Any]) -> float:
        """Extract estimated row count from a plan node"""
        return float(node.get("Plan Rows") or 0.0)

    @staticmethod
    def child_rows(node: Dict[str, Any]) -> List[float]:
        """Get row counts from all child nodes"""
        return [CPUCostCalculator.node_rows(ch) for ch in (node.get("Plans") or [])]

    @staticmethod
    def get_keys_len(arr) -> int:
        """Get length of key array, returning 0 if None"""
        return len(arr) if arr else 0

    def quals_ops_count(self, node: Dict[str, Any]) -> int:
        """
        Count total operators in all qualification clauses of a node.

        Args:
            node: Plan node dictionary

        Returns:
            Total operator count
        """
        total = 0
        qual_keys = [
            "Filter", "Index Cond", "Recheck Cond",
            "Join Filter", "Hash Cond", "Merge Cond"
        ]

        for k in qual_keys:
            v = node.get(k)
            if isinstance(v, list):
                v = " AND ".join(v)
            total += self.count_ops(v)

        return total

    def cpu_cost_node(
        self,
        node: Dict[str, Any],
        guc: Dict[str, float]
    ) -> Tuple[float, List[Tuple[str, float]]]:
        """
        Calculate CPU cost for a plan node and its children.

        Returns:
            Tuple of (inclusive_cpu_cost, breakdown_list)
            where breakdown_list contains (NodeType, self_cpu) tuples
        """
        children = node.get("Plans") or []
        child_costs = []
        total_children = 0.0

        # Recursively calculate children costs
        for ch in children:
            c, br = self.cpu_cost_node(ch, guc)
            child_costs.append((c, br))
            total_children += c

        # Extract cost parameters
        CPU_T = guc["cpu_tuple_cost"]
        CPU_I = guc["cpu_index_tuple_cost"]
        CPU_OP = guc["cpu_operator_cost"]

        ntype = node.get("Node Type", "Unknown")
        rows = self.node_rows(node)
        ops = self.quals_ops_count(node)
        self_cpu = 0.0

        # Calculate CPU cost based on node type
        # Mirroring PostgreSQL's costsize.c formulas

        if ntype == "Seq Scan":
            # cost_seqscan: per-tuple CPU = cpu_tuple_cost + qual operators
            rel_tuples = float(node.get("Plan Rows", 0.0))
            self_cpu += (CPU_T + ops * CPU_OP) * rel_tuples

        elif ntype in ("Index Scan", "Index Only Scan"):
            # cost_index: cpu per tuple includes index and table tuple costs
            candidates = max(rows, 0.0)
            self_cpu += (CPU_I + CPU_T + ops * CPU_OP) * candidates

        elif ntype == "Bitmap Index Scan":
            # cost_bitmap: operator evaluation per candidate
            candidates = max(rows, 0.0)
            self_cpu += ops * CPU_OP * candidates

        elif ntype == "Bitmap Heap Scan":
            # CPU for visibility checks and recheck quals
            candidates = max(rows, 0.0)
            self_cpu += CPU_T * candidates
            self_cpu += ops * CPU_OP * candidates

        elif ntype == "Sort":
            # cost_sort: comparison_cost = 2 * cpu_operator_cost * numSortKeys
            # Total cost = comparison_cost * N * log2(N)
            keys = self.get_keys_len(node.get("Sort Key"))
            n = max(rows, 1.0)
            comparison_cost = 2.0 * CPU_OP * max(1, keys)
            self_cpu += comparison_cost * n * self.log2_safe(n)

        elif ntype == "Hash":
            # Hash table building: touch each input tuple
            cr = self.child_rows(node)
            n_in = cr[0] if cr else rows
            self_cpu += CPU_T * max(n_in, 0.0)

        elif ntype == "Hash Join":
            # cost_hashjoin: hash key computations + tuple processing
            cr = self.child_rows(node)
            outer, inner = 0.0, 0.0

            if children:
                # Identify build (Hash) and probe sides
                for ch, r in zip(children, cr):
                    if ch.get("Node Type") == "Hash":
                        inner = r
                    else:
                        outer = r

                if inner == 0.0 and outer == 0.0 and len(cr) == 2:
                    # Fallback: smaller child is typically build side
                    a, b = cr
                    inner, outer = (a, b) if a <= b else (b, a)

            num_hashclauses = self.count_ops(node.get("Hash Cond"))
            self_cpu += CPU_OP * num_hashclauses              # Build key hashing
            self_cpu += CPU_OP * num_hashclauses * outer      # Probe
            self_cpu += (outer + inner) * CPU_T               # Tuple processing

        elif ntype == "Merge Join":
            # cost_mergejoin: comparisons and tuple processing
            cr = self.child_rows(node)
            total_in = sum(cr) if cr else rows
            num_mergeclauses = self.count_ops(node.get("Merge Cond"))
            self_cpu += total_in * (CPU_OP * max(1, num_mergeclauses))
            self_cpu += total_in * CPU_T

        elif ntype == "Nested Loop":
            # cost_nestloop: tuple processing and qualification
            cr = self.child_rows(node)
            total_in = sum(cr) if cr else rows
            self_cpu += total_in * CPU_T
            self_cpu += total_in * (ops * CPU_OP)

        elif ntype in ("Aggregate", "Group Aggregate"):
            # cost_agg: input processing + grouping
            cr = self.child_rows(node)
            input_tuples = cr[0] if cr else rows
            numGroups = rows if rows > 0 else 1.0
            numGroupCols = self.get_keys_len(node.get("Group Key"))
            self_cpu += CPU_T * input_tuples
            self_cpu += CPU_OP * max(1, numGroupCols) * numGroups

        elif ntype == "HashAggregate":
            # Similar to grouped aggregate but hash-based
            cr = self.child_rows(node)
            input_tuples = cr[0] if cr else rows
            numGroups = rows if rows > 0 else 1.0
            numGroupCols = self.get_keys_len(node.get("Group Key"))
            self_cpu += CPU_T * input_tuples
            self_cpu += CPU_OP * max(1, numGroupCols) * numGroups

        else:
            # Fallback: basic tuple processing
            self_cpu += CPU_T * rows
            self_cpu += ops * CPU_OP * rows

        # Combine self and children costs
        inclusive = total_children + self_cpu
        breakdown = []
        for _, br in child_costs:
            breakdown.extend(br)
        breakdown.append((ntype, self_cpu))

        return inclusive, breakdown

    def calculate_cpu_cost(self, sql: str) -> Optional[float]:
        """
        Calculate the CPU-only cost for a SQL query.

        Args:
            sql: SQL query string

        Returns:
            Total CPU cost as a float, or None if error
        """
        try:
            plan = self.explain_json(sql)
            if plan is None:
                return None

            total_cpu, breakdown = self.cpu_cost_node(plan["Plan"], self.gucs)
            return total_cpu

        except Exception as e:
            print(f"Error calculating CPU cost: {e}")
            return None
