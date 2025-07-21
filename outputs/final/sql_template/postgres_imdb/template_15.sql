-- SQL Template Metadata
-- Template ID: 15
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 1
--   Number of Joins: 2
--   Number of Aggregations: 6
--   Semantic Requirement: The query should use aggregation, and have at least three predicate values to fill.
--   Tables Involved: ['cast_info', 'name', 'person_info']
-- Rewrite Attempts Number for Constraints Check: 2
-- Rewrite Attempts Number for Grammar Check: 1

SELECT COUNT(cast_info.id) AS total_cast,
       AVG(cast_info.nr_order) AS avg_nr_order,
       MIN(cast_info.id) AS min_cast_id,
       MAX(cast_info.id) AS max_cast_id,
       SUM(cast_info.person_role_id) AS total_person_role,
       COUNT(name.id) AS total_name_records
FROM cast_info
JOIN cast_info AS name ON cast_info.person_id = name.person_id
JOIN cast_info AS person_info ON cast_info.person_id = person_info.person_id
WHERE cast_info.id BETWEEN '{{cast_info.id_start}}' AND '{{cast_info.id_end}}'
  AND cast_info.person_role_id = '{{cast_info.person_role_id}}'
  AND person_info.note = '{{person_info.note}}';