-- SQL Template Metadata
-- Template ID: 12
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 1
--   Number of Joins: 0
--   Number of Aggregations: 0
--   Semantic Requirement: The query should use group-by, and have at least two predicate values to fill.
--   Tables Involved: ['keyword']
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 1

SELECT id,
       keyword
FROM keyword
WHERE id >= '{{keyword.id_start}}'
  AND id <= '{{keyword.id_end}}'
GROUP BY id,
         keyword;