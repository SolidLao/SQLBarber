#!/bin/bash

cd ..

echo "=== Running SQLBarber Main Experiments ==="

# ============================================
# IMDB DATABASE EXPERIMENTS
# ============================================
echo ""
echo "=== IMDB Database Experiments ==="

# --- Cardinality Experiments (IMDB) ---
echo "--- Cardinality Experiments (IMDB) ---"

echo "Executing: Cardinality - uniform (IMDB)"
python3 src/run_sqlbarber.py card uniform 1000 0 10000 10 100 imdb

echo "Executing: Cardinality - normal (IMDB)"
python3 src/run_sqlbarber.py card normal 1000 0 10000 10 100 imdb

echo "Executing: Cardinality - Snowset_Card_1_Medium (IMDB)"
python3 src/run_sqlbarber.py card Snowset_Card_1_Medium 1000 0 10000 10 150 imdb

echo "Executing: Cardinality - Snowset_Card_2_Medium (IMDB)"
python3 src/run_sqlbarber.py card Snowset_Card_2_Medium 1000 0 10000 10 150 imdb

echo "Executing: Cardinality - Snowset_Card_1_Hard (IMDB)"
python3 src/run_sqlbarber.py card Snowset_Card_1_Hard 2000 0 10000 20 150 imdb

echo "Executing: Cardinality - Snowset_Card_2_Hard (IMDB)"
python3 src/run_sqlbarber.py card Snowset_Card_2_Hard 2000 0 10000 20 150 imdb

# --- Execution Plan Cost Experiments (IMDB) ---
echo ""
echo "--- Execution Plan Cost Experiments (IMDB) ---"

echo "Executing: Cost - uniform (IMDB)"
python3 src/run_sqlbarber.py cost uniform 1000 0 10000 10 100 imdb

echo "Executing: Cost - normal (IMDB)"
python3 src/run_sqlbarber.py cost normal 1000 0 10000 10 100 imdb

echo "Executing: Cost - Snowset_Cost_Medium (IMDB)"
python3 src/run_sqlbarber.py cost Snowset_Cost_Medium 1000 0 10000 10 150 imdb

echo "Executing: Cost - Redset_Cost_Medium (IMDB)"
python3 src/run_sqlbarber.py cost Redset_Cost_Medium 1000 0 10000 10 150 imdb

echo "Executing: Cost - Snowset_Cost_Hard (IMDB)"
python3 src/run_sqlbarber.py cost Snowset_Cost_Hard 2000 0 10000 20 150 imdb

echo "Executing: Cost - Redset_Cost_Hard (IMDB)"
python3 src/run_sqlbarber.py cost Redset_Cost_Hard 2000 0 10000 20 150 imdb

# ============================================
# TPC-H DATABASE EXPERIMENTS
# ============================================
echo ""
echo "=== TPC-H Database Experiments ==="

# --- Cardinality Experiments (TPC-H) ---
echo "--- Cardinality Experiments (TPC-H) ---"

echo "Executing: Cardinality - uniform (TPC-H)"
python3 src/run_sqlbarber.py card uniform 1000 0 10000 10 100 tpch

echo "Executing: Cardinality - normal (TPC-H)"
python3 src/run_sqlbarber.py card normal 1000 0 10000 10 100 tpch

echo "Executing: Cardinality - Snowset_Card_1_Medium (TPC-H)"
python3 src/run_sqlbarber.py card Snowset_Card_1_Medium 1000 0 10000 10 150 tpch

echo "Executing: Cardinality - Snowset_Card_2_Medium (TPC-H)"
python3 src/run_sqlbarber.py card Snowset_Card_2_Medium 1000 0 10000 10 150 tpch

echo "Executing: Cardinality - Snowset_Card_1_Hard (TPC-H)"
python3 src/run_sqlbarber.py card Snowset_Card_1_Hard 2000 0 10000 20 150 tpch

echo "Executing: Cardinality - Snowset_Card_2_Hard (TPC-H)"
python3 src/run_sqlbarber.py card Snowset_Card_2_Hard 2000 0 10000 20 150 tpch

# --- Execution Plan Cost Experiments (TPC-H) ---
echo ""
echo "--- Execution Plan Cost Experiments (TPC-H) ---"

echo "Executing: Cost - uniform (TPC-H)"
python3 src/run_sqlbarber.py cost uniform 1000 0 10000 10 100 tpch

echo "Executing: Cost - normal (TPC-H)"
python3 src/run_sqlbarber.py cost normal 1000 0 10000 10 100 tpch

echo "Executing: Cost - Snowset_Cost_Medium (TPC-H)"
python3 src/run_sqlbarber.py cost Snowset_Cost_Medium 1000 0 10000 10 150 tpch

echo "Executing: Cost - Redset_Cost_Medium (TPC-H)"
python3 src/run_sqlbarber.py cost Redset_Cost_Medium 1000 0 10000 10 150 tpch

echo "Executing: Cost - Snowset_Cost_Hard (TPC-H)"
python3 src/run_sqlbarber.py cost Snowset_Cost_Hard 2000 0 10000 20 150 tpch

echo "Executing: Cost - Redset_Cost_Hard (TPC-H)"
python3 src/run_sqlbarber.py cost Redset_Cost_Hard 2000 0 10000 20 150 tpch

echo ""
echo "Main experiments completed!"