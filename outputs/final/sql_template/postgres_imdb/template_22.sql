-- SQL Template Metadata
-- Template ID: 22
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 7
--   Number of Joins: 7
--   Number of Aggregations: 5
--   Semantic Requirement: The query should use aggregation, and have at least three predicate values to fill.
--   Tables Involved: [movie_info, aka_name, movie_keyword, title, role_type, movie_companies, company_name, comp_cast_type, company_type, cast_info, movie_info_idx, movie_link, info_type, name, kind_type, keyword, char_name, link_type, aka_title, person_info, complete_cast]
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 2

SELECT MAX(ci.nr_order) AS max_nr_order,
       MIN(mi.info) AS min_info_value,
       COUNT(DISTINCT t.title) AS distinct_titles,
       SUM(mc.company_id) AS sum_company_id,
       AVG(pi.person_id) AS avg_person_id
FROM cast_info AS ci
JOIN movie_info AS mi ON ci.movie_id = mi.movie_id
JOIN title AS t ON mi.movie_id = t.id
JOIN person_info AS pi ON ci.person_id = pi.person_id
JOIN movie_keyword AS mk ON mi.movie_id = mk.movie_id
JOIN movie_companies AS mc ON mi.movie_id = mc.movie_id
JOIN company_name AS cn ON mc.company_id = cn.id
JOIN cast_info AS ci2 ON ci.movie_id = ci2.movie_id
WHERE mi.movie_id = '{{movie_info.movie_id}}'
  AND ci.role_id = '{{cast_info.role_id}}'
  AND pi.person_id BETWEEN '{{person_info.person_id_start}}' AND '{{person_info.person_id_end}}';