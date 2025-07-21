-- Refined SQL Template Metadata
-- Refinement Time: 2025-07-22 03:04:46
-- Operation: both
-- Old Join Path: keyword
-- New Join Path: movie_keyword JOIN keyword
-- Table Size Changes: Changed from using the small 'keyword' table (10 MB, 134K rows) to joining with 'movie_keyword' (360 MB, 4.5M rows) to increase the cost.
-- Structural Changes: Replaced single-table access with a join; modified the predicates to filter on movie_keyword.movie_id with two placeholder values; added an aggregation using COUNT(*).
-- LLM Reasoning: To push the cost into the [5000.0, 6000.0] range, I increased the complexity by joining a larger table ('movie_keyword') with 'keyword'. This join, together with aggregation and filtering on a high-cardinality column, increases the amount of data processed and thus the execution cost.
 -- SQL Template Metadata
-- Template ID: 12
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 2
--   Number of Joins: 1
--   Number of Aggregations: 1
--   Semantic Requirement: The query should use group-by, and have at least two predicate values to fill.
--   Tables Involved: ['movie_keyword', 'keyword']
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 1

SELECT mk.movie_id,
       k.keyword,
       COUNT(*) AS occurrence
FROM movie_keyword mk
JOIN keyword k ON mk.keyword_id = k.id
WHERE mk.movie_id >= '{{movie_keyword.movie_id_start}}'
  AND mk.movie_id <= '{{movie_keyword.movie_id_end}}'
GROUP BY mk.movie_id,
         k.keyword;