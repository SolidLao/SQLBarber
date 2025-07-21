-- SQL Template Metadata
-- Template ID: 9
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 1
--   Number of Joins: 0
--   Number of Aggregations: 0
--   Semantic Requirement: The query should use aggregation, and have at least three predicate values to fill.
--   Tables Involved: ['info_type']
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 2

SELECT info,
       COUNT(id) AS count_id,
       MIN(id) AS min_id,
       MAX(id) AS max_id,
       AVG(id) AS avg_id
FROM info_type
WHERE id BETWEEN '{{info_type.id_start}}' AND '{{info_type.id_end}}'
  AND info = '{{info_type.info}}'
GROUP BY info;