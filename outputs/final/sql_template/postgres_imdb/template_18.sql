-- SQL Template Metadata
-- Template ID: 18
-- Creation Time: 2025-10-15 03:28:41
-- LLM Model: o3-mini
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 0

SELECT movie_info.movie_id,
       title.title AS movie_title,
       COUNT(movie_info.id) AS movie_info_count,
       COUNT(DISTINCT movie_keyword.keyword_id) AS keyword_count,
       AVG(cast_info.nr_order) AS avg_nr_order,
       MAX(company_name.id) AS max_company_id,
       MIN(title.production_year) AS min_production_year,

  (SELECT COUNT(*)
   FROM movie_link
   WHERE movie_link.movie_id = movie_info.movie_id) AS movie_link_count
FROM movie_info
JOIN movie_keyword ON movie_info.movie_id = movie_keyword.movie_id
JOIN movie_companies ON movie_info.movie_id = movie_companies.movie_id
JOIN company_name ON movie_companies.company_id = company_name.id
JOIN title ON movie_info.movie_id = title.id
JOIN cast_info ON title.id = cast_info.movie_id
JOIN aka_name ON cast_info.person_id = aka_name.person_id
WHERE movie_info.info_type_id = '{{movie_info.info_type_id}}'
  AND title.production_year BETWEEN '{{title.production_year_start}}' AND '{{title.production_year_end}}'
GROUP BY movie_info.movie_id,
         title.title;