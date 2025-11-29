-- SQL Template Metadata
-- Template ID: 20
-- Creation Time: 2025-10-15 03:28:41
-- LLM Model: o3-mini
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 4

SELECT char_name.name,
       COUNT(char_name.id) AS char_count,
       MAX(title.production_year) AS max_production_year,
       MIN(cast_info.nr_order) AS min_nr_order
FROM char_name
JOIN title ON char_name.imdb_id = title.imdb_id
JOIN movie_info ON title.id = movie_info.movie_id
JOIN aka_name ON char_name.id = aka_name.person_id
JOIN cast_info ON char_name.id = cast_info.person_id
JOIN movie_keyword ON movie_info.id = movie_keyword.movie_id
JOIN char_name AS cn2 ON char_name.imdb_id = cn2.imdb_id
WHERE char_name.id BETWEEN '{{char_name.id_start}}' AND '{{char_name.id_end}}'
  AND char_name.name = '{{char_name.name}}'
  AND char_name.surname_pcode = '{{char_name.surname_pcode}}'
GROUP BY char_name.name;