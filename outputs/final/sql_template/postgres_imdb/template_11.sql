-- SQL Template Metadata
-- Template ID: 11
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 1

SELECT kind,
       COUNT(*) AS total_cast,

  (SELECT COUNT(*)
   FROM kind_type AS kt2
   WHERE kt2.kind = kind_type.kind
     AND kt2.kind = '{{kind_type.kind}}') AS status_count,

  (SELECT COUNT(*)
   FROM kind_type AS kt3
   WHERE kt3.kind = kind_type.kind
     AND kt3.kind = '{{kind_type.kind}}') AS other_count
FROM kind_type
WHERE kind_type.kind = '{{kind_type.kind}}'
GROUP BY kind;