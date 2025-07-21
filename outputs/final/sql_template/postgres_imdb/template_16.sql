-- SQL Template Metadata
-- Template ID: 16
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 1
--   Number of Joins: 2
--   Number of Aggregations: 6
--   Semantic Requirement: The query should use group-by, and have at least two predicate values to fill.
--   Tables Involved: ['complete_cast', 'name', 'person_info']
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 2

SELECT base.movie_id,
       COUNT(base.id) AS total_cast,
       SUM(base.status_id) AS total_status,
       AVG(base.status_id) AS avg_status,
       MIN(base.status_id) AS min_status,
       MAX(base.status_id) AS max_status,
       COUNT(DISTINCT base.subject_id) AS distinct_subject_count
FROM complete_cast AS base
JOIN name ON base.id = name.id
JOIN person_info ON base.id = person_info.id
WHERE base.status_id BETWEEN '{{complete_cast.status_id_start}}' AND '{{complete_cast.status_id_end}}'
  AND base.subject_id = '{{complete_cast.subject_id}}'
GROUP BY base.movie_id;