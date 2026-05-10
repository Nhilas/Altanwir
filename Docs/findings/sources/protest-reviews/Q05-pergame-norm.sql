-- Q05 — Per-game normalization motivation
-- Show MAX(votesUp) gap between top-volume games and indies
-- Output: G:/Work/Altanwir-scratch/findings-results/Q05-pergame-norm.csv

COPY (
  WITH agg AS (
    SELECT
      r.gameKey,
      g.gameName,
      COUNT(*) AS review_count,
      MAX(r.votesUp) AS max_votes_up,
      ROUND(AVG(r.votesUp::DOUBLE), 2) AS avg_votes_up,
      MAX(r.reviewLength) AS max_review_length
    FROM silver.steamreviews r
    JOIN silver.games g ON g.gameKey = r.gameKey
    GROUP BY r.gameKey, g.gameName
  ),
  high AS (
    SELECT 'high-volume' AS bucket, gameName, review_count, max_votes_up, avg_votes_up, max_review_length
    FROM agg
    ORDER BY review_count DESC
    LIMIT 5
  ),
  low AS (
    SELECT 'low-volume' AS bucket, gameName, review_count, max_votes_up, avg_votes_up, max_review_length
    FROM agg
    WHERE review_count BETWEEN 50 AND 200
    ORDER BY review_count ASC
    LIMIT 5
  )
  SELECT * FROM high
  UNION ALL
  SELECT * FROM low
) TO 'G:/Work/Altanwir-scratch/findings-results/Q05-pergame-norm.csv' (HEADER, DELIMITER ',');

-- Echo
WITH agg AS (
  SELECT
    r.gameKey,
    g.gameName,
    COUNT(*) AS review_count,
    MAX(r.votesUp) AS max_votes_up,
    ROUND(AVG(r.votesUp::DOUBLE), 2) AS avg_votes_up,
    MAX(r.reviewLength) AS max_review_length
  FROM silver.steamreviews r
  JOIN silver.games g ON g.gameKey = r.gameKey
  GROUP BY r.gameKey, g.gameName
),
high AS (
  SELECT 'high-volume' AS bucket, gameName, review_count, max_votes_up, avg_votes_up, max_review_length
  FROM agg
  ORDER BY review_count DESC
  LIMIT 5
),
low AS (
  SELECT 'low-volume' AS bucket, gameName, review_count, max_votes_up, avg_votes_up, max_review_length
  FROM agg
  WHERE review_count BETWEEN 50 AND 200
  ORDER BY review_count ASC
  LIMIT 5
)
SELECT * FROM high
UNION ALL
SELECT * FROM low;
