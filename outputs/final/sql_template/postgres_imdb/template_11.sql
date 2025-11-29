-- SQL Template Metadata
-- Template ID: 11
-- Creation Time: 2025-10-15 03:28:41
-- LLM Model: o3-mini
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 0

SELECT role_id,
       COUNT(id) AS total_ids,
       SUM(person_role_id) AS total_person_role_ids,
       AVG(person_id) AS avg_person_id
FROM cast_info
WHERE movie_id = '{{cast_info.movie_id}}'
  AND role_id BETWEEN '{{cast_info.role_id_start}}' AND '{{cast_info.role_id_end}}'
GROUP BY role_id;