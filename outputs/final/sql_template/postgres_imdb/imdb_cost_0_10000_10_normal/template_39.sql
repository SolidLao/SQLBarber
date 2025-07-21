-- Refined SQL Template Metadata
-- Refinement Time: 2025-07-22 03:05:09
-- Operation: both
-- Old Join Path: movie_info m -> title t -> cast_info c -> role_type r -> movie_companies mc -> company_name cn -> movie_info m2
-- New Join Path: company_name cn -> movie_companies mc -> movie_info m -> title t -> cast_info c -> role_type r -> movie_info m2
-- Table Size Changes: Reordered the joins to start with the smaller company_name (29 MB) and movie_companies (282 MB) before joining the larger tables (movie_info ~1831 MB, cast_info ~3881 MB) to reduce the scanned rows early.
-- Structural Changes: Added an extra predicate on cast_info (c.nr_order = '{{cast_info.nr_order}}') and refined the nested query by including a predicate on movie_companies (mc2.company_type_id = '{{movie_companies.company_type_id}}') to increase selectivity.
-- LLM Reasoning: To push the execution cost into the target range [4000.0, 5000.0], I both re-ordered the join path and added more selective filters. Starting from a smaller table minimizes the intermediate result set and applying selective predicates on high-cardinality tables reduces overall cost. These changes, while retaining the required tables and structure including the nested aggregated subquery, aim to curtail the cost significantly.
 -- SQL Template Metadata
-- Template ID: 20 Refined
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 6
--   Number of Joins: 6
--   Number of Aggregations: 5
--   Semantic Requirement: The query should have a nested query with aggregation, at least two predicate values to fill.
--   Tables Involved: [movie_info, title, cast_info, role_type, movie_companies, company_name]

SELECT m.movie_id,
       COUNT(c.id) AS cast_info_count, -- Aggregation 1
 MIN(t.production_year) AS min_production_year, -- Aggregation 2
 MAX(t.production_year) AS max_production_year, -- Aggregation 3
 AVG(r.id) AS avg_role_id, -- Aggregation 4

  (SELECT SUM(mc2.company_id) -- Aggregation 5 via nested query

   FROM movie_companies mc2
   WHERE mc2.movie_id = m.movie_id
     AND mc2.company_type_id = '{{movie_companies.company_type_id}}') AS nested_sum_company_id
FROM company_name cn
JOIN movie_companies mc ON cn.id = mc.company_id
JOIN movie_info m ON m.movie_id = mc.movie_id
JOIN title t ON m.movie_id = t.id
JOIN cast_info c ON m.movie_id = c.movie_id
JOIN role_type r ON c.role_id = r.id
JOIN movie_info m2 ON m.movie_id = m2.movie_id
WHERE m.movie_id >= '{{movie_info.movie_id_start}}'
  AND m.movie_id <= '{{movie_info.movie_id_end}}'
  AND t.production_year = '{{title.production_year}}'
  AND c.nr_order = '{{cast_info.nr_order}}'
GROUP BY m.movie_id;