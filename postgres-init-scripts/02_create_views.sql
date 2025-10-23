-- Vote counts by pg and ballot
CREATE VIEW pg_vote_count_view AS
WITH vote_counts AS (
    SELECT
        mp_parliamentary_group_memberships.pg_id AS pg_id,
        votes.ballot_id AS ballot_id,
        votes.vote AS vote,
        COUNT(*) AS count
    FROM
        mp_parliamentary_group_memberships
    INNER JOIN votes 
        ON mp_parliamentary_group_memberships.person_id = votes.person_id
    GROUP BY
        mp_parliamentary_group_memberships.pg_id, votes.ballot_id, votes.vote
)
SELECT
    pg_id,
    ballot_id,
    vote,
    count
FROM vote_counts
ORDER BY pg_id, ballot_id, count DESC;


-- Most popular vote by pg and ballot
CREATE VIEW pg_mode_vote_view AS
SELECT DISTINCT ON (pg_id, ballot_id) -- Takes first unique pair of each combination
    pg_id,
    ballot_id,
    vote AS mode_vote,
    count
FROM pg_vote_count_view
ORDER BY pg_id, ballot_id, count DESC;


--Contra vote score by mp
CREATE MATERIALIZED VIEW contra_vote_scores_view AS
WITH pg_mode_and_person_vote AS (
    SELECT 
        pg_modes.mode_vote,
        pg_modes.pg_id,
        pg_modes.ballot_id,
        votes.vote,
        mp_parliamentary_group_memberships.person_id
    FROM ((pg_mode_vote_view AS pg_modes
    INNER JOIN mp_parliamentary_group_memberships 
        ON mp_parliamentary_group_memberships.pg_id=pg_modes.pg_id)    -- Join pg_memberships and mode_vote in order to 
    INNER JOIN votes                                                -- Join individual votes
        ON pg_modes.ballot_id=votes.ballot_id 
        AND votes.person_id = mp_parliamentary_group_memberships.person_id) 
)
SELECT 
    person_id, 
    CAST(SUM(CASE WHEN vote = mode_vote OR vote = 'absent' THEN 0 ELSE 1 END) AS FLOAT) / NULLIF(COUNT(vote), 0) AS contra_vote_score
FROM pg_mode_and_person_vote
GROUP BY person_id;