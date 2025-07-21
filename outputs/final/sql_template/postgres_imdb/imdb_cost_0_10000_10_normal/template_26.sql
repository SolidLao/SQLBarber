-- Refined SQL Template Metadata
-- Refinement Time: 2025-07-22 03:04:46
-- Operation: both
-- Old Join Path: keyword
-- New Join Path: keyword -> movie_keyword -> movie_info
-- Table Size Changes: Changed from a small table (keyword, ~10 MB) to joining movie_keyword (~360 MB) and movie_info (~1831 MB) to increase the cost due to larger data volumes and join complexity.
-- Structural Changes: Added two JOINs to include movie_keyword and movie_info; added an extra predicate on mi.info_type_id; extended the GROUP BY clause to include the new column. This increases processing complexity.
-- LLM Reasoning: To shift the execution plan cost into the target [6000.0, 7000.0] range, I modified the join path to include larger tables (movie_keyword and movie_info) and increased the structural complexity by adding an additional predicate. The combination of additional joins, larger tables, and extra filtering results in a higher processing cost while still satisfying the original semantic requirements.
 -- SQL Template Metadata
-- Template ID: 12
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 1 (originally) but now increased via join
--   Number of Joins: 2
--   Number of Aggregations: 0
--   Semantic Requirement: The query should use group-by, and have at least two predicate values to fill.
--   Tables Involved: ['keyword', 'movie_keyword', 'movie_info']
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 1

SELECT k.id,
       k.keyword,
       mi.info
FROM keyword AS k
JOIN movie_keyword AS mk ON k.id = mk.keyword_id
JOIN movie_info AS mi ON mk.movie_id = mi.movie_id
WHERE k.id >= '{{keyword.id_start}}'
  AND k.id <= '{{keyword.id_end}}'
  AND mi.info_type_id = '{{movie_info.info_type_id}}'
GROUP BY k.id,
         k.keyword,
         mi.info;