-- SQL Template Metadata
-- Template ID: 19
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 7
--   Number of Joins: 6
--   Number of Aggregations: 5
--   Semantic Requirement: The query should use group-by, and have at least two predicate values to fill.
--   Tables Involved: ['movie_info', 'aka_name', 'movie_keyword', 'title', 'role_type', 'movie_companies', 'company_name', 'comp_cast_type', 'company_type', 'cast_info', 'movie_info_idx', 'movie_link', 'info_type', 'name', 'kind_type', 'keyword', 'char_name', 'link_type', 'aka_title', 'person_info', 'complete_cast']

SELECT COUNT(DISTINCT title.id) AS total_titles,
       AVG(title.production_year) AS avg_prod_year,
       MAX(movie_keyword.keyword_id) AS max_keyword,
       SUM(cast_info.nr_order) AS sum_nr_order,

  (SELECT COUNT(*)
   FROM movie_info) AS total_movie_info_count
FROM title
JOIN movie_info ON title.id = movie_info.movie_id
JOIN movie_keyword ON title.id = movie_keyword.movie_id
JOIN movie_companies ON title.id = movie_companies.movie_id
JOIN company_name ON movie_companies.company_id = company_name.id
JOIN cast_info ON title.id = cast_info.movie_id
JOIN cast_info AS cast_info2 ON title.id = cast_info2.movie_id
WHERE title.production_year BETWEEN '{{'{{title.production_year_start}}'}}' AND '{{'{{title.production_year_end}}'}}'
  AND movie_info.info_type_id = '{{'{{movie_info.info_type_id}}'}}';