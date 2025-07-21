-- Refined SQL Template Metadata
-- Refinement Time: 2025-07-22 03:04:43
-- Operation: both
-- Old Join Path: complete_cast
-- New Join Path: complete_cast JOIN movie_info ON complete_cast.movie_id = movie_info.movie_id
-- Table Size Changes: We added movie_info, a much larger table (1831 MB, 14,835,720 rows) compared to complete_cast (11 MB, 135086 rows) to increase the overall query cost.
-- Structural Changes: Added a JOIN with movie_info along with an additional predicate on movie_info.info_type_id. The nested subquery was updated accordingly to include the join, ensuring the constraint on nested aggregation remains intact.
-- LLM Reasoning: To push the cost into the target range [4000.0, 5000.0], the strategy was to increase query complexity by joining with a significantly larger table (movie_info) and adding extra filtering conditions. This increases both the IO and CPU cost while maintaining the nested aggregation constraint.
 -- SQL Template Metadata
-- Template ID: 6
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: increased from 1 to 2
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
WHERE cc.id >= '{{complete_cast.id_start}}'
  AND cc.id <= '{{complete_cast.id_end}}'
  AND cc.status_id = '{{complete_cast.status_id}}'
  AND mi.info_type_id = '{{movie_info.info_type_id}}'
  AND cc.id >
    (SELECT AVG(cc2.id)
     FROM complete_cast cc2
     JOIN movie_info mi2 ON cc2.movie_id = mi2.movie_id
     WHERE cc2.id >= '{{complete_cast.id_start}}'
       AND mi2.info_type_id = '{{movie_info.info_type_id}}')
GROUP BY cc.status_id;