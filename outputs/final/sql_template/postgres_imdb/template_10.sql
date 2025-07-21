-- SQL Template Metadata
-- Template ID: 10
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 1
--   Number of Joins: 0
--   Number of Aggregations: 0
--   Semantic Requirement: The query should use aggregation, and have at least three predicate values to fill.
--   Tables Involved: ['aka_title']
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 4

SELECT *
FROM aka_title
WHERE title <> '{{aka_title.title_exclude}}'
  AND id >
    (SELECT AVG(id)
     FROM aka_title
     WHERE id > '{{aka_title.id_start}}'::integer
       AND id < '{{aka_title.id_end}}'::integer);