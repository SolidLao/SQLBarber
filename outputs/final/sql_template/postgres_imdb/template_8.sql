-- SQL Template Metadata
-- Template ID: 8
-- Creation Time: 2025-10-15 03:28:41
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 1
--   Number of Joins: 0
--   Number of Aggregations: 0
--   Semantic Requirement: The query should have a nested query with aggregation, at least two predicate values to fill.
--   Tables Involved: ['complete_cast']
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 1

SELECT sub.total_count,
       sub.avg_movie_id,
       sub.sum_status_id
FROM
  (SELECT COUNT(*) AS total_count,
          AVG(movie_id) AS avg_movie_id,
          SUM(status_id) AS sum_status_id
   FROM complete_cast
   WHERE movie_id BETWEEN '{{complete_cast.movie_id_start}}' AND '{{complete_cast.movie_id_end}}'
     AND status_id = '{{complete_cast.status_id}}') sub;