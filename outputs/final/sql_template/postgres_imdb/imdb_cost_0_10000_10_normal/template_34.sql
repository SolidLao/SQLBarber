-- Refined SQL Template Metadata
-- Refinement Time: 2025-07-22 03:04:47
-- Operation: both
-- Old Join Path: name
-- New Join Path: name JOIN cast_info ON name.id = cast_info.person_id
-- Table Size Changes: Changed from a single table 'name' (524 MB, 4M+ rows) to joining 'name' with 'cast_info' (3881 MB, 36M+ rows) to significantly increase processing complexity.
-- Structural Changes: Added a join with an extra large table, introduced an additional predicate on cast_info.role_id, and added an aggregation (COUNT) to enforce a GROUP BY, thus increasing the query cost.
-- LLM Reasoning: To push the cost into the target range of [7000.0, 8000.0], the query structure was modified by joining with a massive table (cast_info) and aggregating data, which is expected to substantially increase the execution plan cost. Additional predicates and grouping ensure that the cost is driven upward while still meeting the template constraints.
 -- SQL Template Metadata
-- Template ID: 8-refined
-- Creation Time: {{current_timestamp}}
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 2
--   Number of Joins: 1
--   Number of Aggregations: 1
--   Semantic Requirement: The query should use group-by, and have at least two predicate values to fill.
--   Tables Involved: ['name', 'cast_info']
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 1

SELECT n.id,
       n.name,
       n.imdb_id,
       COUNT(c.id) AS cast_count
FROM name n
JOIN cast_info c ON n.id = c.person_id
WHERE n.id BETWEEN '{{name.id_start}}' AND '{{name.id_end}}'
  AND n.gender = '{{name.gender}}'
  AND c.role_id = '{{cast_info.role_id}}'
GROUP BY n.id,
         n.name,
         n.imdb_id;