-- SQL Template Metadata
-- Template ID: 17
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 1

SELECT title.title,
       COUNT(movie_info.id) AS movie_count,
       AVG(movie_keyword.keyword_id) AS avg_keyword,
       MIN(title.production_year) AS min_production_year,
       MAX(cast_info.nr_order) AS max_nr_order,
       SUM(movie_companies.company_type_id) AS total_company_type,

  (SELECT COUNT(*)
   FROM cast_info AS ci
   WHERE ci.movie_id = title.id) AS nested_cast_count
FROM movie_info
JOIN title ON movie_info.movie_id = title.id
JOIN movie_keyword ON movie_info.movie_id = movie_keyword.movie_id
JOIN movie_companies ON movie_info.movie_id = movie_companies.movie_id
JOIN cast_info ON movie_info.movie_id = cast_info.movie_id
JOIN company_name ON movie_companies.company_id = company_name.id
JOIN person_info ON cast_info.person_id = person_info.person_id
WHERE movie_info.info_type_id = '{{movie_info.info_type_id}}'
  AND title.production_year BETWEEN '{{title.production_year_start}}' AND '{{title.production_year_end}}'
GROUP BY title.title,
         title.id;