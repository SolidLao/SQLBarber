-- SQL Template Metadata
-- Template ID: 17
-- Creation Time: 2025-10-15 03:28:41
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 7
--   Number of Joins: 6
--   Number of Aggregations: 5
--   Semantic Requirement: The query should use aggregation, and have at least three predicate values to fill.
--   Tables Involved: [movie_info, aka_name, movie_keyword, title, role_type, movie_companies, company_name, comp_cast_type, company_type, cast_info, movie_info_idx, movie_link, info_type, name, kind_type, keyword, char_name, link_type, aka_title, person_info, complete_cast]
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 0

SELECT movie_info.movie_id,
       COUNT(DISTINCT movie_keyword.keyword_id) AS keyword_count,
       AVG(title.production_year) AS avg_year,
       MIN(company_name.id) AS min_company,
       MAX(cast_info.nr_order) AS max_nr_order,
       SUM(person_info.person_id) AS sum_person_id
FROM movie_info
JOIN movie_keyword ON movie_info.movie_id = movie_keyword.movie_id
JOIN title ON movie_info.movie_id = title.id
JOIN movie_companies ON movie_info.movie_id = movie_companies.movie_id
JOIN company_name ON movie_companies.company_id = company_name.id
JOIN cast_info ON movie_info.movie_id = cast_info.movie_id
JOIN person_info ON cast_info.person_id = person_info.person_id
WHERE movie_info.info_type_id = '{{movie_info.info_type_id}}'
  AND title.production_year BETWEEN '{{title.production_year_start}}' AND '{{title.production_year_end}}'
  AND company_name.country_code = '{{company_name.country_code}}'
GROUP BY movie_info.movie_id;