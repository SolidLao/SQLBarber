-- SQL Template Metadata
-- Template ID: 22
-- Creation Time: 2025-10-15 03:28:41
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 7
--   Number of Joins: 7
--   Number of Aggregations: 5
--   Semantic Requirement: The query should have a nested query with aggregation, at least two predicate values to fill.
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 1

SELECT title.id,
       title.title,
       COUNT(DISTINCT movie_info.info) AS info_count,
       SUM(cast_info.nr_order) AS total_nr_order,
       MIN(title.production_year) AS min_production_year,
       MAX(movie_keyword.keyword_id) AS max_keyword_id,

  (SELECT AVG(mc.company_type_id)
   FROM movie_companies mc
   WHERE mc.movie_id = title.id) AS avg_company_type
FROM title
JOIN movie_info ON title.id = movie_info.movie_id
JOIN movie_keyword ON title.id = movie_keyword.movie_id
JOIN cast_info ON title.id = cast_info.movie_id
JOIN movie_companies ON title.id = movie_companies.movie_id
JOIN company_name ON movie_companies.company_id = company_name.id
JOIN aka_name ON title.imdb_index = aka_name.imdb_index
JOIN movie_info movie_info2 ON title.id = movie_info2.movie_id
WHERE title.production_year BETWEEN '{{title.production_year_start}}' AND '{{title.production_year_end}}'
  AND movie_keyword.keyword_id = '{{movie_keyword.keyword_id}}'
GROUP BY title.id,
         title.title;