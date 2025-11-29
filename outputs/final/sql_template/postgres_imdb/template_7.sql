-- SQL Template Metadata
-- Template ID: 7
-- Creation Time: 2025-10-15 03:28:41
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 1
--   Number of Joins: 0
--   Number of Aggregations: 0
--   Semantic Requirement: The query should have a nested query with aggregation, at least two predicate values to fill.
--   Tables Involved: ['role_type']
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 1

SELECT ROLE,
       COUNT(*) AS role_count,

  (SELECT AVG(id)
   FROM role_type t2
   WHERE t2.role = t1.role
     AND t2.id >= '{{role_type.id_start}}'
     AND t2.id <= '{{role_type.id_end}}') AS avg_id
FROM role_type t1
WHERE id >= '{{role_type.id_start}}'
  AND id <= '{{role_type.id_end}}'
GROUP BY ROLE;