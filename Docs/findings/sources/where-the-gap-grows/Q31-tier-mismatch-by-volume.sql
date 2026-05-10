-- Q31 — Tier-mismatch rate per volume bucket
-- Of games rated on BOTH sentiment and vote axes (i.e. both tiers not Insufficient),
-- what share land a different tier on each axis, and which way does the gap lean?
-- Output: G:/Work/Altanwir-scratch/findings-results/Q31-tier-mismatch-by-volume.csv

WITH ranked AS (
  SELECT
    CASE
      WHEN totalReviews < 100 THEN '1: 0-99'
      WHEN totalReviews < 1000 THEN '2: 100-999'
      WHEN totalReviews < 10000 THEN '3: 1k-9.9k'
      WHEN totalReviews < 100000 THEN '4: 10k-99k'
      ELSE '5: 100k+'
    END AS volume_bucket,
    weightedSentimentTier AS s_tier,
    weightedVoteTier AS v_tier,
    CASE weightedSentimentTier
      WHEN 'S' THEN 1 WHEN 'A' THEN 2 WHEN 'B' THEN 3
      WHEN 'C' THEN 4 WHEN 'D' THEN 5 WHEN 'F' THEN 6
    END AS s_ord,
    CASE weightedVoteTier
      WHEN 'S' THEN 1 WHEN 'A' THEN 2 WHEN 'B' THEN 3
      WHEN 'C' THEN 4 WHEN 'D' THEN 5 WHEN 'F' THEN 6
    END AS v_ord
  FROM gold.vw_factGameScores
  WHERE totalReviews IS NOT NULL
    AND weightedSentimentTier IS NOT NULL
    AND weightedVoteTier IS NOT NULL
    AND weightedSentimentTier <> 'Insufficient Data'
    AND weightedVoteTier <> 'Insufficient Data'
)
SELECT
  volume_bucket,
  COUNT(*) AS games_rated_both,
  ROUND(100.0 * SUM(CASE WHEN s_ord = v_ord THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_same_tier,
  ROUND(100.0 * SUM(CASE WHEN v_ord < s_ord THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_vote_tier_higher,
  ROUND(100.0 * SUM(CASE WHEN s_ord < v_ord THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_sentiment_tier_higher
FROM ranked
GROUP BY 1
ORDER BY 1;

COPY (
  WITH ranked AS (
    SELECT
      CASE
        WHEN totalReviews < 100 THEN '1: 0-99'
        WHEN totalReviews < 1000 THEN '2: 100-999'
        WHEN totalReviews < 10000 THEN '3: 1k-9.9k'
        WHEN totalReviews < 100000 THEN '4: 10k-99k'
        ELSE '5: 100k+'
      END AS volume_bucket,
      CASE weightedSentimentTier
        WHEN 'S' THEN 1 WHEN 'A' THEN 2 WHEN 'B' THEN 3
        WHEN 'C' THEN 4 WHEN 'D' THEN 5 WHEN 'F' THEN 6
      END AS s_ord,
      CASE weightedVoteTier
        WHEN 'S' THEN 1 WHEN 'A' THEN 2 WHEN 'B' THEN 3
        WHEN 'C' THEN 4 WHEN 'D' THEN 5 WHEN 'F' THEN 6
      END AS v_ord
    FROM gold.vw_factGameScores
    WHERE totalReviews IS NOT NULL
      AND weightedSentimentTier IS NOT NULL
      AND weightedVoteTier IS NOT NULL
      AND weightedSentimentTier <> 'Insufficient Data'
      AND weightedVoteTier <> 'Insufficient Data'
  )
  SELECT
    volume_bucket,
    COUNT(*) AS games_rated_both,
    ROUND(100.0 * SUM(CASE WHEN s_ord = v_ord THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_same_tier,
    ROUND(100.0 * SUM(CASE WHEN v_ord < s_ord THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_vote_tier_higher,
    ROUND(100.0 * SUM(CASE WHEN s_ord < v_ord THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_sentiment_tier_higher
  FROM ranked
  GROUP BY 1
  ORDER BY 1
) TO 'G:/Work/Altanwir-scratch/findings-results/Q31-tier-mismatch-by-volume.csv' (HEADER, DELIMITER ',');
