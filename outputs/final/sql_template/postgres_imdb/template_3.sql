-- SQL Template Metadata
-- Template ID: 3
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 1
--   Number of Joins: 0
--   Number of Aggregations: 0
--   Semantic Requirement: The query should use group-by, and have at least two predicate values to fill.
--   Tables Involved: ['info_type']
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 1

SELECT id,
       info
FROM info_type
WHERE id = '{{info_type.id}}'
  AND info = '{{info_type.info}}'
GROUP BY id,
         info;