BEGIN;

-- add `search_vector` column
ALTER TABLE proposals
  ADD COLUMN IF NOT EXISTS search_vector tsvector;

-- function to refresh all proposals' `search_vector`s
CREATE OR REPLACE FUNCTION refresh_all_proposals_search_vector()
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  rows_updated INTEGER;
BEGIN
  WITH signer_agg AS (
    SELECT
      ps.proposal_id,
      string_agg(persons.first_name || ' ' || persons.last_name, ' ') AS signer_names
    FROM proposal_signatures ps
    JOIN persons ON persons.id = ps.person_id
    GROUP BY ps.proposal_id
  ),
  built AS (
    SELECT
      pr.id,
      to_tsvector(
        'finnish',
        COALESCE(pr.title, '') || ' ' ||
        COALESCE(pr.summary, '') || ' ' ||
        COALESCE(pr.reasoning, '') || ' ' ||
        COALESCE(pr.law_changes, '') || ' ' ||
        COALESCE(sa.signer_names, '')
      ) AS vect
    FROM proposals pr
    LEFT JOIN signer_agg sa ON sa.proposal_id = pr.id
  )
  UPDATE proposals p
  SET search_vector = b.vect
  FROM built b
  WHERE p.id = b.id
    AND (p.search_vector IS DISTINCT FROM b.vect);

  GET DIAGNOSTICS rows_updated = ROW_COUNT;
  RETURN rows_updated;
END;
$$;

-- GIN index for fast full-text search
CREATE INDEX IF NOT EXISTS proposals_search_idx
  ON proposals USING GIN (search_vector);

-- convenience search function
CREATE OR REPLACE FUNCTION search_proposals(q TEXT, limit_rows INT DEFAULT 50, offset_rows INT DEFAULT 0)
RETURNS TABLE (
  id VARCHAR,
  ptype proposal_type,
  date DATE,
  title TEXT,
  content_snippet TEXT,
  status handling_status,
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

  SELECT COUNT(*) INTO total FROM proposals p WHERE p.search_vector @@ q_ts;

  RETURN QUERY
  SELECT
    p.id,
    p.ptype,
    p.date,
    ts_headline('finnish', COALESCE(p.title, ''), q_ts, hl_title_opts) AS title,

    CASE
      WHEN to_tsvector('finnish', COALESCE(p.summary, '')) @@ q_ts
        THEN ts_headline('finnish', p.summary, q_ts, hl_opts)
      WHEN to_tsvector('finnish', COALESCE(p.reasoning, '')) @@ q_ts
        THEN ts_headline('finnish', p.reasoning, q_ts, hl_opts)
      WHEN to_tsvector('finnish', COALESCE(p.law_changes, '')) @@ q_ts
        THEN ts_headline('finnish', p.law_changes, q_ts, hl_opts)
      ELSE p.summary
    END AS content_snippet,

    p.status,
    (ts_rank_cd(p.search_vector, q_ts))::double precision AS rank,
    total AS total_hits
  FROM proposals p
  WHERE p.search_vector @@ q_ts
  ORDER BY rank DESC, date DESC
  LIMIT limit_rows
  OFFSET offset_rows;
END;
$$;

-- initialize/populate existing proposals' `search_vector`s
SELECT refresh_all_proposals_search_vector();

COMMIT;
