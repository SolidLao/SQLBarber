-- SQL Template Metadata
-- Template ID: 19
-- Creation Time: 2025-10-15 03:28:41
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 7
--   Number of Joins: 6
--   Number of Aggregations: 5
--   Semantic Requirement: The query should use group-by, and have at least two predicate values to fill.
--   Tables Involved: ['movie_info', 'aka_name', 'movie_keyword', 'title', 'role_type', 'movie_companies', 'company_name', 'comp_cast_type', 'company_type', 'cast_info', 'movie_info_idx', 'movie_link', 'info_type', 'name', 'kind_type', 'keyword', 'char_name', 'link_type', 'aka_title', 'person_info', 'complete_cast']
-- Rewrite Attempts Number for Constraints Check: 0
-- Rewrite Attempts Number for Grammar Check: 0

SELECT cn.name,
       COUNT(DISTINCT mi.id) AS movie_count,
       AVG(t.production_year) AS avg_production_year,
       MIN(pi.person_id) AS min_person_id,
       MAX(mk.keyword_id) AS max_keyword_id,
       SUM(ci.nr_order) AS total_order
FROM movie_info mi
JOIN title t ON mi.movie_id = t.id
JOIN movie_keyword mk ON t.id = mk.movie_id
JOIN cast_info ci ON t.id = ci.movie_id
JOIN person_info pi ON ci.person_id = pi.person_id
JOIN movie_companies mc ON t.id = mc.movie_id
JOIN company_name cn ON mc.company_id = cn.id
WHERE t.production_year >= '{{title.production_year_start}}'
  AND t.production_year <= '{{title.production_year_end}}'
  AND mi.info_type_id = '{{movie_info.info_type_id}}'
GROUP BY cn.name;