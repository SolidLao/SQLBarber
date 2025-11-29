-- SQL Template Metadata
-- Template ID: 1
-- Creation Time: 2025-10-15 03:28:41
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 1
--   Number of Joins: 0
--   Number of Aggregations: 0
--   Semantic Requirement: The query should use aggregation, and have at least three predicate values to fill.
--   Tables Involved: ['info_type']
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 1

SELECT info,
       COUNT(id) AS total
FROM info_type
WHERE id >= '{{info_type.id_start}}'
  AND id <= '{{info_type.id_end}}'
  AND info = '{{info_type.info}}'
GROUP BY info;