-- SQL Template Metadata
-- Template ID: 9
-- Creation Time: 2025-10-15 03:28:41
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 1
--   Number of Joins: 0
--   Number of Aggregations: 0
--   Semantic Requirement: The query should use aggregation, and have at least three predicate values to fill.
--   Tables Involved: [comp_cast_type]
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 2

SELECT kind,
       COUNT(id) AS id_count
FROM comp_cast_type
WHERE kind = '{{comp_cast_type.kind}}'
  AND id >= '{{comp_cast_type.id_start}}'
  AND id <= '{{comp_cast_type.id_end}}'
GROUP BY kind;