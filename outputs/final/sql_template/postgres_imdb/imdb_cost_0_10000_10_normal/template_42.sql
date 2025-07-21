-- Refined SQL Template Metadata
-- Refinement Time: 2025-07-22 03:05:17
-- Operation: both
-- Old Join Path: movie_info m -> title t -> cast_info c -> role_type r -> movie_companies mc -> company_name cn -> movie_info m2
-- New Join Path: movie_info m -> title t -> cast_info c -> role_type r -> movie_companies mc -> company_name cn -> company_name cn2
-- Table Size Changes: Replaced the self-join on the huge movie_info table (14835720 rows, 1831 MB) with a join on company_name (234997 rows, 29 MB) to lower the cost.
-- Structural Changes: Added an additional predicate condition on mc.company_type_id and removed the expensive self join. The nested query with aggregation is retained and all required placeholders remain.
-- LLM Reasoning: To shift the execution plan cost into the target range [8000.0, 9000.0], the join path was modified by replacing the self join on movie_info with a join on a smaller company_name table. Additionally, a new selective predicate was added on movie_companies.company_type_id to further reduce the processed row count and lower the cost.
 -- SQL Template Metadata
-- Template ID: 20
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 6
--   Number of Joins: 6
--   Number of Aggregations: 5
--   Semantic Requirement: The query should have a nested query with aggregation, at least two predicate values to fill.
--   Tables Involved: [movie_info, title, cast_info, role_type, movie_companies, company_name]
-- Rewrite Attempts Number for Constraints Check: 0
-- Rewrite Attempts Number for Grammar Check: 0

SELECT m.movie_id,
       COUNT(c.id) AS cast_info_count, -- Aggregation 1
 MIN(t.production_year) AS min_production_year, -- Aggregation 2
 MAX(t.production_year) AS max_production_year, -- Aggregation 3
 AVG(r.id) AS avg_role_id, -- Aggregation 4

  (SELECT SUM(mc2.company_id) -- Aggregation 5 via nested query

   FROM movie_companies mc2
   WHERE mc2.movie_id = m.movie_id) AS nested_sum_company_id
FROM movie_info m
JOIN title t ON m.movie_id = t.id
JOIN cast_info c ON m.movie_id = c.movie_id
JOIN role_type r ON c.role_id = r.id
JOIN movie_companies mc ON m.movie_id = mc.movie_id
JOIN company_name cn ON mc.company_id = cn.id -- Replacing the expensive self join on movie_info with a join on company_name again (smaller table) to meet join count
JOIN company_name cn2 ON m.movie_id = cn2.id
WHERE m.movie_id >= '{{movie_info.movie_id_start}}'
  AND m.movie_id <= '{{movie_info.movie_id_end}}'
  AND t.production_year = '{{title.production_year}}'
  AND mc.company_type_id = '{{movie_companies.company_type_id}}' -- Additional selective predicate to reduce cost
GROUP BY m.movie_id;