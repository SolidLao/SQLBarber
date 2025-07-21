-- Refined SQL Template Metadata
-- Refinement Time: 2025-07-22 03:04:51
-- Operation: both
-- Old Join Path: complete_cast
-- New Join Path: complete_cast JOIN movie_info
-- Table Size Changes: Added movie_info (size: 1831 MB, row_count: 14835720) in the join to increase the computational cost.
-- Structural Changes: Introduced a JOIN with movie_info on movie_id and added a new predicate on mi.info_type_id; maintained the nested query with aggregation condition.
-- LLM Reasoning: To shift the execution cost into the target range [3000.0, 4000.0], a join with a larger table (movie_info) was introduced, slightly increasing the query complexity and cost while keeping the original constraints and nested subquery structure intact.
 -- SQL Template Metadata
-- Template ID: 6
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 2
--   Number of Joins: 1
--   Number of Aggregations: 2
--   Semantic Requirement: The query should have a nested query with aggregation, at least two predicate values to fill.
--   Tables Involved: ['complete_cast', 'movie_info']
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 2

SELECT cc.status_id,
       COUNT(*) AS total_count
FROM complete_cast cc
JOIN movie_info mi ON cc.movie_id = mi.movie_id
AND mi.info_type_id = '{{movie_info.info_type_id}}'
WHERE cc.id >= '{{complete_cast.id_start}}'
  AND cc.id <= '{{complete_cast.id_end}}'
  AND cc.status_id = '{{complete_cast.status_id}}'
  AND cc.id >
    (SELECT AVG(id)
     FROM complete_cast
     WHERE id >= '{{complete_cast.id_start}}')
GROUP BY cc.status_id;