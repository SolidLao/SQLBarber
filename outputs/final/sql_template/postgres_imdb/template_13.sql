-- SQL Template Metadata
-- Template ID: 13
-- Creation Time: 2025-10-15 03:28:41
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 1
--   Number of Joins: 0
--   Number of Aggregations: 2
--   Semantic Requirement: The query should use aggregation, and have at least three predicate values to fill.
--   Tables Involved: ['title']
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 1

SELECT title.id,
       COUNT(*) AS total_records,
       AVG(title.production_year) AS avg_production_year
FROM title
WHERE title.production_year BETWEEN '{{title.production_year_start}}' AND '{{title.production_year_end}}'
  AND title.episode_nr BETWEEN '{{title.episode_nr_start}}' AND '{{title.episode_nr_end}}'
  AND title.season_nr BETWEEN '{{title.season_nr_start}}' AND '{{title.season_nr_end}}'
GROUP BY title.id;