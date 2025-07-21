-- SQL Template Metadata
-- Template ID: 1
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 1
--   Number of Joins: 0
--   Number of Aggregations: 0
--   Semantic Requirement: The query should have a nested query with aggregation, at least two predicate values to fill.
--   Tables Involved: ['kind_type']
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 4

SELECT *
FROM kind_type
WHERE id BETWEEN CAST('{{kind_type.id_start}}' AS BIGINT) AND CAST('{{kind_type.id_end}}' AS BIGINT)
  AND kind IN
    (SELECT kind
     FROM kind_type
     GROUP BY kind
     HAVING MAX(id) > CAST('{{kind_type.id_start}}' AS BIGINT));