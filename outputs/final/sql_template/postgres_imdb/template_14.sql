-- SQL Template Metadata
-- Template ID: 14
-- Creation Time: 2025-10-15 03:28:41
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 2
--   Number of Joins: 1
--   Number of Aggregations: 0
--   Semantic Requirement: The query should use group-by, and have at least two predicate values to fill.
--   Tables Involved: ['cast_info', 'title']
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 1

SELECT cast_info.person_id,
       title.production_year
FROM cast_info
JOIN title ON cast_info.movie_id = title.id
WHERE cast_info.role_id = '{{cast_info.role_id}}'
  AND title.production_year BETWEEN '{{title.production_year_start}}' AND '{{title.production_year_end}}'
GROUP BY cast_info.person_id,
         title.production_year;