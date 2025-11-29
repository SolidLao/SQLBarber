-- SQL Template Metadata
-- Template ID: 12
-- Creation Time: 2025-10-15 03:28:41
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 1
--   Number of Joins: 0
--   Number of Aggregations: 0
--   Semantic Requirement: The query should use group-by, and have at least two predicate values to fill.
--   Tables Involved: ['movie_info']
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 1

SELECT movie_info.info_type_id
FROM movie_info
WHERE movie_info.info_type_id BETWEEN '{{movie_info.info_type_id_start}}' AND '{{movie_info.info_type_id_end}}'
GROUP BY movie_info.info_type_id;