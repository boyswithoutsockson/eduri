
-- Most popular vote by pg and ballot
CREATE VIEW mode_votes_view AS
WITH vote_counts AS (
    SELECT
        mp_parliamentary_group_memberships.pg_id AS pg_id,
        votes.ballot_id AS ballot_id,
        votes.vote AS mode_vote,
        COUNT(*) AS count
    FROM
        mp_parliamentary_group_memberships
    INNER JOIN votes ON mp_parliamentary_group_memberships.person_id = votes.person_id
    GROUP BY
        mp_parliamentary_group_memberships.pg_id, votes.ballot_id, votes.vote
)
SELECT DISTINCT ON (pg_id, ballot_id)
    pg_id,
    ballot_id,
    mode_vote,
    count
FROM vote_counts
ORDER BY pg_id, ballot_id, count DESC;


--Contra vote score by mp
CREATE MATERIALIZED VIEW contra_vote_scores_view AS
WITH vote_modes AS (
    SELECT 
        mode_vote,
        modes.pg_id,
        modes.ballot_id,
        votes.vote AS vote,
        mp_parliamentary_group_memberships.person_id AS person_id
    FROM ((mode_votes_view AS modes
    INNER JOIN mp_parliamentary_group_memberships ON mp_parliamentary_group_memberships.pg_id=modes.pg_id)
    INNER JOIN votes ON votes.ballot_id=modes.ballot_id AND votes.person_id = mp_parliamentary_group_memberships.person_id)
)
SELECT vote_modes.person_id, CAST(SUM(CASE WHEN vote_modes.vote = vote_modes.mode_vote OR vote_modes.vote = 'absent' THEN 0 ELSE 1 END) AS FLOAT) / NULLIF(COUNT(vote_modes.vote), 0) AS contra_vote_score
FROM vote_modes
GROUP BY vote_modes.person_id;