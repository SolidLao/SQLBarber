-- SQL Template Metadata
-- Template ID: 6
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 1
--   Number of Joins: 0
--   Number of Aggregations: 2
--   Semantic Requirement: The query should have a nested query with aggregation, at least two predicate values to fill.
--   Tables Involved: ['complete_cast']
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 2

SELECT status_id,
       COUNT(*) AS total_count
FROM complete_cast
WHERE id >= '{{complete_cast.id_start}}'
  AND id <= '{{complete_cast.id_end}}'
  AND status_id = '{{complete_cast.status_id}}'
  AND id >
    (SELECT AVG(id)
     FROM complete_cast
     WHERE id >= '{{complete_cast.id_start}}')
GROUP BY status_id;