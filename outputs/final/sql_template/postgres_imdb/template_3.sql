-- SQL Template Metadata
-- Template ID: 3
-- Creation Time: 2025-10-15 03:28:41
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 1
--   Number of Joins: 0
--   Number of Aggregations: 0
--   Semantic Requirement: The query should use aggregation, and have at least three predicate values to fill.
--   Tables Involved: ['char_name']
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 4

SELECT char_name.id,
       char_name.name,
       COUNT(char_name.id) AS name_count
FROM char_name
WHERE char_name.id BETWEEN '{{char_name.id_start}}' AND '{{char_name.id_end}}'
  AND char_name.imdb_index = '{{char_name.imdb_index}}'
  AND char_name.md5sum = '{{char_name.md5sum}}'
GROUP BY char_name.id,
         char_name.name;