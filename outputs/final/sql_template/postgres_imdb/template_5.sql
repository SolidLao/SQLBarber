-- SQL Template Metadata
-- Template ID: 5
-- Creation Time: 2025-10-15 03:28:41
-- LLM Model: o3-mini
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 1

SELECT sub.total_count,
       sub.avg_id
FROM
  (SELECT COUNT(id) AS total_count,
          AVG(id) AS avg_id
   FROM name
   WHERE id >= '{{name.id_start}}'
     AND id <= '{{name.id_end}}') sub
WHERE sub.total_count > 0;