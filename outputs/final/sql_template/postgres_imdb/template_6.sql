-- SQL Template Metadata
-- Template ID: 6
-- Creation Time: 2025-10-15 03:28:41
-- LLM Model: o3-mini
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 1

SELECT *
FROM title
WHERE id IN
    (SELECT MAX(id)
     FROM title
     WHERE id >= '{{title.id_start}}'
       AND id <= '{{title.id_end}}'
     GROUP BY kind_id);