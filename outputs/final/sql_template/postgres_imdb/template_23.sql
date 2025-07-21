-- SQL Template Metadata
-- Template ID: 23
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 7
--   Number of Joins: 7
--   Number of Aggregations: 5
--   Semantic Requirement: The query should use aggregation, and have at least three predicate values to fill.
--   Tables Involved: ['movie_info', 'aka_name', 'movie_keyword', 'title', 'role_type', 'movie_companies', 'company_name', 'comp_cast_type', 'company_type', 'cast_info', 'movie_info_idx', 'movie_link', 'info_type', 'name', 'kind_type', 'keyword', 'char_name', 'link_type', 'aka_title', 'person_info', 'complete_cast']
-- Rewrite Attempts Number for Constraints Check: 0
-- Rewrite Attempts Number for Grammar Check: 0

SELECT COUNT(mi.id) AS total_movies,
       COUNT(m2.id) AS self_movie_count,
       AVG(mk.keyword_id) AS avg_keyword_id,
       MAX(t.production_year) AS max_production_year,
       MIN(ci.nr_order) AS min_nr_order
FROM movie_info mi
JOIN movie_keyword mk ON mi.movie_id = mk.movie_id
JOIN title t ON mi.movie_id = t.id
JOIN movie_companies mc ON mi.movie_id = mc.movie_id
JOIN company_name cn ON mc.company_id = cn.id
JOIN cast_info ci ON mi.movie_id = ci.movie_id
JOIN aka_name an ON ci.person_id = an.person_id
JOIN movie_info m2 ON mi.movie_id = m2.movie_id
WHERE mi.info_type_id = '{{movie_info.info_type_id}}'
  AND t.production_year BETWEEN '{{title.production_year_start}}' AND '{{title.production_year_end}}'
  AND cn.country_code = '{{company_name.country_code}}';