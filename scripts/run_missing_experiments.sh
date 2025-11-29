#!/bin/bash

cd ..

echo "=== Running Missing TPC-H Experiments ==="

echo "Executing: python3 src/run_sqlbarber.py cost Redset_Cost_Hard 2000 0 10000 20 150 tpch10"
python3 src/run_sqlbarber.py cost Redset_Cost_Hard 2000 0 10000 20 150 tpch10

echo "Executing: python3 src/run_sqlbarber.py cost Redset_Cost_Hard 2000 0 10000 20 150 tpch10"
python3 src/run_sqlbarber.py cost Redset_Cost_Hard 2000 0 10000 20 150 tpch10

echo "Executing: python3 src/run_sqlbarber.py cost Redset_Cost_Hard 2000 0 10000 20 150 tpch10"
python3 src/run_sqlbarber.py cost Redset_Cost_Hard 2000 0 10000 20 150 tpch10

echo "Executing: python3 src/run_sqlbarber.py cost Redset_Cost_Hard 2000 0 10000 20 150 tpch20"
python3 src/run_sqlbarber.py cost Redset_Cost_Hard 2000 0 10000 20 150 tpch20

echo "Executing: python3 src/run_sqlbarber.py cost Redset_Cost_Hard 2000 0 10000 20 150 tpch20"
python3 src/run_sqlbarber.py cost Redset_Cost_Hard 2000 0 10000 20 150 tpch20

echo "Executing: python3 src/run_sqlbarber.py cost Redset_Cost_Hard 2000 0 10000 20 150 tpch20"
python3 src/run_sqlbarber.py cost Redset_Cost_Hard 2000 0 10000 20 150 tpch20

echo "Executing: python3 src/run_sqlbarber.py cost Snowset_Cost_Hard 2000 0 10000 20 150 tpch50"
python3 src/run_sqlbarber.py cost Snowset_Cost_Hard 2000 0 10000 20 150 tpch50

echo "Executing: python3 src/run_sqlbarber.py cost Redset_Cost_Hard 2000 0 10000 20 150 tpch50"
python3 src/run_sqlbarber.py cost Redset_Cost_Hard 2000 0 10000 20 150 tpch50

echo "Executing: python3 src/run_sqlbarber.py cost Redset_Cost_Hard 2000 0 10000 20 150 tpch50"
python3 src/run_sqlbarber.py cost Redset_Cost_Hard 2000 0 10000 20 150 tpch50

echo "Executing: python3 src/run_sqlbarber.py cost Redset_Cost_Hard 2000 0 10000 20 150 tpch50"
python3 src/run_sqlbarber.py cost Redset_Cost_Hard 2000 0 10000 20 150 tpch50

echo "Missing experiments completed!"
