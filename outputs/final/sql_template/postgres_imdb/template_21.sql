-- SQL Template Metadata
-- Template ID: 21
-- Creation Time: 2025-10-15 03:28:41
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 7
--   Number of Joins: 6
--   Number of Aggregations: 5
--   Semantic Requirement: The query should use group-by, and have at least two predicate values to fill.
--   Tables Involved: ['movie_info', 'aka_name', 'movie_keyword', 'title', 'movie_companies', 'company_name', 'cast_info']
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 0

SELECT movie_info.movie_id,
       MIN(movie_keyword.keyword_id) AS min_keyword,
       MAX(title.production_year) AS max_year,
       AVG(cast_info.nr_order) AS avg_order,
       SUM(movie_companies.company_type_id) AS sum_company_type,

  (SELECT COUNT(*)
   FROM cast_info ci2
   WHERE ci2.movie_id = movie_info.movie_id) AS cast_count
FROM movie_info
JOIN movie_keyword ON movie_info.movie_id = movie_keyword.movie_id
JOIN title ON movie_info.movie_id = title.id
JOIN movie_companies ON movie_info.movie_id = movie_companies.movie_id
JOIN company_name ON movie_companies.company_id = company_name.id
JOIN cast_info ON movie_info.movie_id = cast_info.movie_id
JOIN aka_name ON cast_info.person_id = aka_name.person_id
WHERE movie_info.info_type_id = '{{movie_info.info_type_id}}'
  AND title.production_year BETWEEN '{{title.production_year_start}}' AND '{{title.production_year_end}}'
GROUP BY movie_info.movie_id;