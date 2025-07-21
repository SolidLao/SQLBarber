-- SQL Template Metadata
-- Template ID: 7
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 1
--   Number of Joins: 0
--   Number of Aggregations: 0
--   Semantic Requirement: The query should use aggregation, and have at least three predicate values to fill.
--   Tables Involved: ['comp_cast_type']
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 0

SELECT
  (SELECT COUNT(*)
   FROM comp_cast_type
   WHERE kind = '{{comp_cast_type.kind}}') AS total_kind,
       t.min_id,
       t.max_id
FROM
  (SELECT MIN(id) AS min_id,
          MAX(id) AS max_id
   FROM comp_cast_type
   WHERE id BETWEEN '{{comp_cast_type.id_start}}' AND '{{comp_cast_type.id_end}}') t;