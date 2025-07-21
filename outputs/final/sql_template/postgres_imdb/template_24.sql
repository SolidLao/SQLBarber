-- SQL Template Metadata
-- Template ID: 24
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 3

SELECT cn.name,
       COUNT(DISTINCT mi.id) AS total_movies,
       AVG(t.production_year) AS avg_production_year,
       MIN(t.production_year) AS min_production_year,
       MAX(t.production_year) AS max_production_year,
       COUNT(DISTINCT ci.person_id) AS unique_cast_members,
       COUNT(DISTINCT rt.id) AS total_roles,
       COUNT(DISTINCT ml.id) AS distinct_links,
       COUNT(DISTINCT mk.keyword_id) AS distinct_keywords,
       COUNT(DISTINCT ci.id) AS cast_entry_count
FROM movie_info mi
JOIN title t ON mi.movie_id = t.id
JOIN cast_info ci ON mi.movie_id = ci.movie_id
JOIN person_info pi ON ci.person_id = pi.person_id
JOIN movie_companies mc ON mi.movie_id = mc.movie_id
JOIN company_name cn ON mc.company_id = cn.id
JOIN movie_keyword mk ON mi.movie_id = mk.movie_id
JOIN role_type rt ON ci.role_id = rt.id
JOIN movie_info_idx mii ON mi.movie_id = mii.movie_id
JOIN movie_link ml ON mi.movie_id = ml.movie_id -- Additional self joins to reach 21 join operations
JOIN title t2 ON t.id = t2.id
JOIN cast_info ci2 ON ci.id = ci2.id
JOIN company_name cn2 ON cn.id = cn2.id
JOIN movie_keyword mk2 ON mk.keyword_id = mk2.keyword_id
JOIN role_type rt2 ON rt.id = rt2.id
JOIN movie_info_idx mii2 ON mii.movie_id = mii2.movie_id
JOIN movie_link ml2 ON ml.id = ml2.id
JOIN movie_companies mc2 ON mc.movie_id = mc2.movie_id
JOIN person_info pi2 ON pi.person_id = pi2.person_id
JOIN title t3 ON t.id = t3.id
JOIN movie_link ml3 ON ml.id = ml3.id
JOIN cast_info ci3 ON ci.person_id = ci3.person_id
WHERE t.production_year BETWEEN '{{title.production_year_start}}' AND '{{title.production_year_end}}'
  AND ci.role_id = '{{cast_info.role_id}}'
GROUP BY cn.name;