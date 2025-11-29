-- SQL Template Metadata
-- Template ID: 4
-- Creation Time: 2025-10-15 03:28:41
-- LLM Model: o3-mini
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 1

SELECT mc.movie_id,
       mc.company_id,
       mc.note,
       agg.total_companies
FROM movie_companies mc
JOIN
  (SELECT movie_id,
          COUNT(id) AS total_companies
   FROM movie_companies
   WHERE movie_id = '{{movie_companies.movie_id}}'
   GROUP BY movie_id) AS agg ON mc.movie_id = agg.movie_id
WHERE mc.company_id BETWEEN '{{movie_companies.company_id_start}}' AND '{{movie_companies.company_id_end}}';