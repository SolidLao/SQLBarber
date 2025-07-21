-- Refined SQL Template Metadata
-- Refinement Time: 2025-07-22 03:04:48
-- Operation: both
-- Old Join Path: Accessing table 'name' only
-- New Join Path: Join between 'name' and 'cast_info' on n.id = ci.person_id
-- Table Size Changes: Added join with 'cast_info', a very large table (3881 MB, 36244344 rows) to increase the overall execution cost towards the target range.
-- Structural Changes: Added a join and an extra predicate on cast_info.role_id, and included an aggregation (COUNT) in the SELECT clause to increase query complexity.
-- LLM Reasoning: To raise the execution plan cost from the historical low range into [8000.0, 9000.0], we incorporated a join with the large table 'cast_info' and introduced an additional predicate. This structural enhancement and join adjustment significantly increases the query's computational complexity and cost, while still satisfying the original group-by and predicate placeholder constraints.
 -- SQL Template Metadata
-- Template ID: 8
-- Creation Time: 2025-07-22 02:58:48
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
       COUNT(ci.id) AS movie_count
FROM name n
JOIN cast_info ci ON n.id = ci.person_id 
WHERE n.id BETWEEN '{{name.id_start}}' AND '{{name.id_end}}' 
  AND n.gender = '{{name.gender}}' 
  AND ci.role_id = '{{cast_info.role_id}}'
GROUP BY n.id,
         n.name,
         n.imdb_id;