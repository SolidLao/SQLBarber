-- SQL Template Metadata
-- Template ID: 13
-- Creation Time: 2025-07-22 02:58:48
-- LLM Model: o3-mini
-- Rewrite Attempts Number for Constraints Check: 3
-- Rewrite Attempts Number for Grammar Check: 1

SELECT *
FROM movie_info_idx
WHERE id > '{{movie_info_idx.id}}';