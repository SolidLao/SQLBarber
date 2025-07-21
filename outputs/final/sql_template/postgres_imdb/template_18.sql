-- SQL Template Metadata
-- Template ID: 18
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 7
--   Number of Joins: 6
--   Number of Aggregations: 5
--   Semantic Requirement: The query should use group-by, and have at least two predicate values to fill.
--   Tables Involved: ['movie_info', 'aka_name', 'movie_keyword', 'title', 'role_type', 'movie_companies', 'company_name', 'comp_cast_type', 'company_type', 'cast_info', 'movie_info_idx', 'movie_link', 'info_type', 'name', 'kind_type', 'keyword', 'char_name', 'link_type', 'aka_title', 'person_info', 'complete_cast']
-- Rewrite Attempts Number for Constraints Check: 0
-- Rewrite Attempts Number for Grammar Check: 0

SELECT title.title,
       COUNT(DISTINCT cast_info.person_id) AS cast_count,
       AVG(movie_info_idx.info_type_id) AS avg_info_type,
       MAX(movie_keyword.keyword_id) AS max_keyword,
       SUM(movie_companies.company_id) AS sum_company,
       MIN(movie_info.id) AS min_info_id,

  (SELECT COUNT(*)
   FROM cast_info AS ci
   WHERE ci.movie_id = movie_info.movie_id) AS nested_cast_count
FROM title
JOIN movie_info ON movie_info.movie_id = title.id
JOIN movie_info_idx ON movie_info.movie_id = movie_info_idx.movie_id
JOIN movie_keyword ON movie_keyword.movie_id = movie_info.movie_id
JOIN cast_info ON cast_info.movie_id = movie_info.movie_id
JOIN movie_companies ON movie_companies.movie_id = movie_info.movie_id
JOIN company_name ON company_name.id = movie_companies.company_id
WHERE title.production_year BETWEEN '{{title.production_year_start}}' AND '{{title.production_year_end}}'
GROUP BY title.title,
         movie_info.movie_id;