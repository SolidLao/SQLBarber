-- SQL Template Metadata
-- Template ID: 16
-- Creation Time: 2025-10-15 03:28:41
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 1
--   Number of Joins: 2
--   Number of Aggregations: 6
--   Semantic Requirement: The query should use group-by, and have at least two predicate values to fill.
--   Tables Involved: ['cast_info', 'name', 'person_info']
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 1

SELECT ci.person_role_id AS group_column,
       COUNT(ci.id) AS total_count,
       SUM(ci.nr_order) AS total_nr_order,
       AVG(ci.nr_order) AS avg_nr_order,
       MIN(ci.nr_order) AS min_nr_order,
       MAX(ci.nr_order) AS max_nr_order,
       COUNT(DISTINCT ci.role_id) AS distinct_roles
FROM cast_info AS ci
JOIN cast_info AS t1 ON ci.person_id = t1.person_id
JOIN cast_info AS t2 ON ci.person_id = t2.person_id
WHERE ci.id BETWEEN '{{cast_info.id_start}}' AND '{{cast_info.id_end}}'
  AND ci.note = '{{cast_info.note}}'
GROUP BY ci.person_role_id;