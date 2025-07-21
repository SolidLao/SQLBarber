#!/bin/bash

cd ..

echo "=== Running Scalability Experiments (IMDB only) ==="

# ============================================
# SCALABILITY EXPERIMENTS - IMDB DATABASE ONLY
# ============================================

# --- Scalability: Query Count Scaling ---
echo ""
echo "--- Scalability: Query Count Scaling (IMDB) ---"

echo "Executing: Query Scalability - 50 queries (IMDB)"
python3 src/run_sqlbarber.py cost Redset_Cost_Scalability_Query_50 50 0 10000 10 150 imdb

echo "Executing: Query Scalability - 500 queries (IMDB)"
python3 src/run_sqlbarber.py cost Redset_Cost_Scalability_Query_500 500 0 10000 10 150 imdb

echo "Executing: Query Scalability - 5000 queries (IMDB)"
python3 src/run_sqlbarber.py cost Redset_Cost_Scalability_Query_5000 5000 0 10000 10 150 imdb

# --- Scalability: Interval Count Scaling ---
echo ""
echo "--- Scalability: Interval Count Scaling (IMDB) ---"

echo "Executing: Interval Scalability - 5 intervals (IMDB)"
python3 src/run_sqlbarber.py cost Redset_Cost_Scalability_Interval_5 1000 0 10000 5 150 imdb

echo "Executing: Interval Scalability - 10 intervals (IMDB)"
python3 src/run_sqlbarber.py cost Redset_Cost_Scalability_Interval_10 1000 0 10000 10 150 imdb

echo "Executing: Interval Scalability - 15 intervals (IMDB)"
python3 src/run_sqlbarber.py cost Redset_Cost_Scalability_Interval_15 1000 0 10000 15 150 imdb

echo "Executing: Interval Scalability - 20 intervals (IMDB)"
python3 src/run_sqlbarber.py cost Redset_Cost_Scalability_Interval_20 1000 0 10000 20 150 imdb

echo "Executing: Interval Scalability - 25 intervals (IMDB)"
python3 src/run_sqlbarber.py cost Redset_Cost_Scalability_Interval_25 1000 0 10000 25 150 imdb

echo ""
echo "Scalability experiments completed!"