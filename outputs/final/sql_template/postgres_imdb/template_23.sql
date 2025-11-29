-- SQL Template Metadata
-- Template ID: 23
-- Creation Time: 2025-10-15 03:28:41
-- LLM Model: o3-mini
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 0

SELECT COUNT(mi.movie_id) AS count_mi,
       SUM(mk.keyword_id) AS sum_mk,
       AVG(mc.movie_id) AS avg_mc,
       MIN(cn.id) AS min_cn,
       MAX(ci.id) AS max_ci
FROM movie_info mi
JOIN title t ON mi.movie_id = t.id
JOIN movie_keyword mk ON t.id = mk.movie_id
JOIN movie_companies mc ON t.id = mc.movie_id
JOIN company_name cn ON mc.company_id = cn.id
JOIN cast_info ci ON t.id = ci.movie_id
JOIN person_info pi ON ci.person_id = pi.person_id
JOIN movie_info mi2 ON mi.id = mi2.id
WHERE t.production_year >= '{{title.production_year_start}}'
  AND mc.company_type_id = '{{movie_companies.company_type_id}}'
  AND ci.role_id = '{{cast_info.role_id}}';