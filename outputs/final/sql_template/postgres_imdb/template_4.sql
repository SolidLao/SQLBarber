-- SQL Template Metadata
-- Template ID: 4
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 1
--   Number of Joins: 0
--   Number of Aggregations: 0
--   Semantic Requirement: The query should have a nested query with aggregation, at least two predicate values to fill.
--   Tables Involved: ['role_type']
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 3

SELECT id,
       ROLE
FROM role_type
WHERE id >= CAST('{{role_type.id_start}}' AS INTEGER)
  AND id <= CAST('{{role_type.id_end}}' AS INTEGER)
  AND
    (SELECT COUNT(*)
     FROM role_type AS sub
     WHERE sub.role = role_type.role) >= CAST('{{role_type.id}}' AS INTEGER);