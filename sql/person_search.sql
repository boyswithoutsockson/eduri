BEGIN;

-- add `search_vector` column for persons (members of parliament)
ALTER TABLE persons
  ADD COLUMN IF NOT EXISTS search_vector tsvector;

-- function to refresh all persons' `search_vector`s
CREATE OR REPLACE FUNCTION refresh_all_persons_search_vector()
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  rows_updated INTEGER;
BEGIN
  WITH latest_pg AS (
    -- pick the membership with the latest end_date per person (NULLS LAST)
    SELECT DISTINCT ON (m.person_id)
      m.person_id,
      pg.id AS pg_id,
      pg.name AS pg_name
    FROM mp_parliamentary_group_memberships m
    JOIN parliamentary_groups pg ON pg.id = m.pg_id
    ORDER BY m.person_id, m.end_date DESC NULLS LAST, m.start_date DESC
  ),
  latest_minister AS (
    -- pick the latest minister position per person (NULLS LAST)
    SELECT DISTINCT ON (mi.person_id)
      mi.person_id,
      mi.minister_position
    FROM ministers mi
    ORDER BY mi.person_id, mi.end_date DESC NULLS LAST, mi.start_date DESC
  ),
  built AS (
    SELECT
      p.id,
      to_tsvector(
        'finnish',
        COALESCE(p.first_name, '') || ' ' ||
        COALESCE(p.last_name, '') || ' ' ||
        COALESCE(lp.pg_name, '') || ' ' ||
        COALESCE(lm.minister_position, '')
      ) AS vect
    FROM persons p
    LEFT JOIN latest_pg lp ON lp.person_id = p.id
    LEFT JOIN latest_minister lm ON lm.person_id = p.id
  )
  UPDATE persons p
  SET search_vector = b.vect
  FROM built b
  WHERE p.id = b.id
    AND (p.search_vector IS DISTINCT FROM b.vect);

  GET DIAGNOSTICS rows_updated = ROW_COUNT;
  RETURN rows_updated;
END;
$$;

-- GIN index for fast full-text search on persons
CREATE INDEX IF NOT EXISTS persons_search_idx
  ON persons USING GIN (search_vector);

-- convenience search function for persons
CREATE OR REPLACE FUNCTION search_persons(q TEXT, limit_rows INT DEFAULT 50, offset_rows INT DEFAULT 0)
RETURNS TABLE (
  id INT,
  first_name TEXT,
  last_name TEXT,
  photo TEXT,
  email TEXT,
  occupation TEXT,
  place_of_residence TEXT,
  party_id VARCHAR,
  minister_position TEXT,
  rank DOUBLE PRECISION,
  total_hits BIGINT
) LANGUAGE plpgsql STABLE AS $$
DECLARE
  q_ts tsquery;
  hl_opts TEXT := 'StartSel=<mark>, StopSel=</mark>, MaxFragments=1, FragmentDelimiter='' [...] '', MaxWords=30, MinWords=3, ShortWord=0';
  hl_title_opts TEXT := 'StartSel=<mark>, StopSel=</mark>, HighlightAll=TRUE';
  total BIGINT;
BEGIN
  IF q IS NULL OR btrim(q) = '' THEN
    RETURN;
  END IF;

  q_ts := websearch_to_tsquery('finnish', q);

  SELECT COUNT(*) INTO total FROM persons p WHERE p.search_vector @@ q_ts;

  RETURN QUERY
  WITH latest_pg AS (
    SELECT DISTINCT ON (m.person_id)
      m.person_id,
      pg.id AS pg_id,
      pg.name AS pg_name
    FROM mp_parliamentary_group_memberships m
    JOIN parliamentary_groups pg ON pg.id = m.pg_id
    ORDER BY m.person_id, m.end_date DESC NULLS LAST, m.start_date DESC
  ),
  latest_minister AS (
    SELECT DISTINCT ON (mi.person_id)
      mi.person_id,
      mi.minister_position
    FROM ministers mi
    ORDER BY mi.person_id, mi.end_date DESC NULLS FIRST, mi.start_date DESC
  ),
  candidate_matches AS (
    SELECT
      p.id::int,
      p.first_name::text,
      p.last_name::text,
      p.photo::text,
      p.email::text,
      p.occupation::text,
      p.place_of_residence::text,
      lp.pg_id::varchar AS party_id,
      lm.minister_position::text,
      -- Exact match on whole words gets highest boost
      (CASE
        WHEN p.first_name ILIKE q OR p.last_name ILIKE q THEN 1.0
        ELSE 0.0
      END +
      -- Prefix match (starts with query)
      CASE
        WHEN p.first_name ILIKE q || '%' OR p.last_name ILIKE q || '%' THEN 0.5
        ELSE 0.0
      END +
      -- Substring match anywhere in the word (for partial matches like "berg" in "bergbom")
      CASE
        WHEN p.first_name ILIKE '%' || q || '%' OR p.last_name ILIKE '%' || q || '%' THEN 0.1
        ELSE 0.0
      END +
      -- Full-text search rank
      (ts_rank_cd(p.search_vector, q_ts) * 5.0)
      ) AS combined_rank
    FROM persons p
    LEFT JOIN latest_pg lp ON lp.person_id = p.id
    LEFT JOIN latest_minister lm ON lm.person_id = p.id
    WHERE p.search_vector @@ q_ts
      OR p.first_name ILIKE '%' || q || '%'
      OR p.last_name ILIKE '%' || q || '%'
  )
  SELECT
    candidate_matches.id,
    candidate_matches.first_name,
    candidate_matches.last_name,
    candidate_matches.photo,
    candidate_matches.email,
    candidate_matches.occupation,
    candidate_matches.place_of_residence,
    candidate_matches.party_id,
    candidate_matches.minister_position,
    candidate_matches.combined_rank::double precision,
    (SELECT COUNT(DISTINCT candidate_matches.id) FROM candidate_matches)::bigint
  FROM candidate_matches
  ORDER BY combined_rank DESC, candidate_matches.last_name ASC NULLS LAST
  LIMIT limit_rows
  OFFSET offset_rows;
END;
$$;

-- initialize/populate existing persons' `search_vector`s
SELECT refresh_all_persons_search_vector();

COMMIT;
