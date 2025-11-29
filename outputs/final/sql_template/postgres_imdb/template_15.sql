-- SQL Template Metadata
-- Template ID: 15
-- Creation Time: 2025-10-15 03:28:41
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 1
--   Number of Joins: 2
--   Number of Aggregations: 6
--   Semantic Requirement: The query should use group-by, and have at least two predicate values to fill.
--   Tables Involved: ['movie_companies', 'title', 'movie_link']
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 1

SELECT mc.company_id,
       COUNT(*) AS total_records,
       SUM(mc.movie_id) AS sum_movie_id,
       AVG(mc.movie_id) AS avg_movie_id,
       MIN(mc.movie_id) AS min_movie_id,
       MAX(mc.movie_id) AS max_movie_id,
       COUNT(DISTINCT mc.movie_id) AS distinct_movies
FROM movie_companies mc
JOIN title t ON mc.movie_id = t.id
JOIN movie_link ml ON t.id = ml.movie_id
WHERE mc.company_type_id = '{{movie_companies.company_type_id}}'
  AND mc.movie_id = '{{movie_companies.movie_id}}'
GROUP BY mc.company_id;