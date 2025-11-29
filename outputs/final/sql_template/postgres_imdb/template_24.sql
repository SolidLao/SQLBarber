-- SQL Template Metadata
-- Template ID: 24
-- Creation Time: 2025-10-15 03:28:41
-- LLM Model: o3-mini
-- Constraints:
--   Number of unique Tables Accessed: 10
--   Number of Joins: 21
--   Number of Aggregations: 16
--   Semantic Requirement: The query should have a nested query with aggregation, at least two predicate values to fill.
--   Tables Involved: [movie_info, aka_name, movie_keyword, title, role_type, movie_companies, company_name, comp_cast_type, company_type, cast_info, movie_info_idx, movie_link, info_type, name, kind_type, keyword, char_name, link_type, aka_title, person_info, complete_cast]
-- Rewrite Attempts Number for Constraints Check: 1
-- Rewrite Attempts Number for Grammar Check: 2

SELECT COUNT(DISTINCT mi1.movie_id) AS total_movies,
       AVG(title.production_year) AS avg_production_year,
       MIN(cast_info.nr_order) AS min_nr_order,
       MAX(movie_keyword.keyword_id) AS max_keyword,

  (SELECT AVG(actor_count)
   FROM
     (SELECT COUNT(*) AS actor_count
      FROM cast_info
      GROUP BY movie_id) sub) AS avg_actor_count,
       COUNT(DISTINCT t2.id) AS distinct_t2_id,
       SUM(ci2.nr_order) AS sum_nr_order_ci2,
       MIN(pi2.person_id) AS min_pi_person_id,
       MAX(mk3.keyword_id) AS max_mk3_keyword_id,
       AVG(mc2.movie_id) AS avg_mc2_movie,
       COUNT(an2.id) AS count_aka_names,
       SUM(rt2.id) AS sum_rt2,
       AVG(cct2.id) AS avg_cct2,
       COUNT(mi3.movie_id) AS count_mi3,
       SUM(t3.production_year) AS sum_t3_prod_year
FROM movie_info mi1
JOIN title ON mi1.movie_id = title.id
JOIN cast_info ON title.id = cast_info.movie_id
JOIN person_info ON cast_info.person_id = person_info.person_id
JOIN movie_keyword ON mi1.movie_id = movie_keyword.movie_id
JOIN movie_companies ON mi1.movie_id = movie_companies.movie_id
JOIN company_name ON movie_companies.company_id = company_name.id
JOIN aka_name ON person_info.person_id = aka_name.person_id
JOIN role_type ON cast_info.role_id = role_type.id
JOIN comp_cast_type ON cast_info.person_role_id = comp_cast_type.id
JOIN movie_info mi2 ON mi1.movie_id = mi2.movie_id
JOIN title t2 ON mi1.movie_id = t2.id
JOIN cast_info ci2 ON mi1.movie_id = ci2.movie_id
JOIN person_info pi2 ON ci2.person_id = pi2.person_id
JOIN movie_keyword mk3 ON mi1.movie_id = mk3.movie_id
JOIN movie_companies mc2 ON mi1.movie_id = mc2.movie_id
JOIN company_name cn2 ON mc2.company_id = cn2.id
JOIN aka_name an2 ON person_info.person_id = an2.person_id
JOIN role_type rt2 ON ci2.role_id = rt2.id
JOIN comp_cast_type cct2 ON ci2.person_role_id = cct2.id
JOIN movie_info mi3 ON mi1.movie_id = mi3.movie_id
JOIN title t3 ON mi1.movie_id = t3.id
WHERE title.production_year BETWEEN '{{title.production_year_start}}' AND '{{title.production_year_end}}';