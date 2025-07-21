-- SQL Template Metadata
-- Template ID: 14
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 2
--   Number of Joins: 1
--   Number of Aggregations: 0
--   Semantic Requirement: The query should use aggregation, and have at least three predicate values to fill.
--   Tables Involved: ['cast_info', 'title']
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 4

SELECT AVG(title.production_year) AS avg_year
FROM cast_info
JOIN title ON cast_info.movie_id = title.id
WHERE cast_info.role_id = {{cast_info.role_id}}
  AND title.kind_id = {{title.kind_id}}
  AND title.imdb_id = {{title.imdb_id}};