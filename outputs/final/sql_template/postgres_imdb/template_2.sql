-- SQL Template Metadata
-- Template ID: 2
-- Creation Time: 2025-10-15 03:28:41
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 1
--   Number of Joins: 0
--   Number of Aggregations: 1
--   Semantic Requirement: The query should use aggregation, and have at least three predicate values to fill.
--   Tables Involved: ['name']
-- Rewrite Attempts Number for Constraints Check: 2
-- Rewrite Attempts Number for Grammar Check: 1

SELECT id,
       gender
FROM name
WHERE id >
    (SELECT AVG(id)
     FROM name
     WHERE id >= '{{name.id_start}}'
       AND id <= '{{name.id_end}}')
  AND gender = '{{name.gender}}';