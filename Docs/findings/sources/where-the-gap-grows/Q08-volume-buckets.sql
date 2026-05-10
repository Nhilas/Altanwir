-- Q08 — Volume bucket reveal: vote climbs, sentiment stays spread
-- Output: G:/Work/Altanwir-scratch/findings-results/Q08-volume-buckets.csv

COPY (
  SELECT
    CASE
      WHEN totalReviews < 100 THEN '1: 0-99'
      WHEN totalReviews < 1000 THEN '2: 100-999'
      WHEN totalReviews < 10000 THEN '3: 1k-9.9k'
      WHEN totalReviews < 100000 THEN '4: 10k-99k'
      ELSE '5: 100k+'
    END AS volume_bucket,
    COUNT(*) AS games,
    ROUND(AVG(weightedSentimentRating), 2) AS avg_sentiment_rating,
    ROUND(AVG(weightedVoteRating), 2) AS avg_vote_rating,
    ROUND(AVG(sentimentVoteAlignment), 2) AS avg_alignment,
    ROUND(STDDEV_POP(weightedSentimentRating), 2) AS stddev_sentiment,
    ROUND(STDDEV_POP(weightedVoteRating), 2) AS stddev_vote,
    ROUND(STDDEV_POP(sentimentVoteAlignment), 2) AS stddev_alignment
  FROM gold.vw_factGameScores
  WHERE totalReviews IS NOT NULL
  GROUP BY 1
  ORDER BY 1
) TO 'G:/Work/Altanwir-scratch/findings-results/Q08-volume-buckets.csv' (HEADER, DELIMITER ',');

-- Echo
SELECT
  CASE
    WHEN totalReviews < 100 THEN '1: 0-99'
    WHEN totalReviews < 1000 THEN '2: 100-999'
    WHEN totalReviews < 10000 THEN '3: 1k-9.9k'
    WHEN totalReviews < 100000 THEN '4: 10k-99k'
    ELSE '5: 100k+'
  END AS volume_bucket,
  COUNT(*) AS games,
  ROUND(AVG(weightedSentimentRating), 2) AS avg_sentiment_rating,
  ROUND(AVG(weightedVoteRating), 2) AS avg_vote_rating,
  ROUND(AVG(sentimentVoteAlignment), 2) AS avg_alignment,
  ROUND(STDDEV_POP(weightedSentimentRating), 2) AS stddev_sentiment,
  ROUND(STDDEV_POP(weightedVoteRating), 2) AS stddev_vote,
  ROUND(STDDEV_POP(sentimentVoteAlignment), 2) AS stddev_alignment
FROM gold.vw_factGameScores
WHERE totalReviews IS NOT NULL
GROUP BY 1
ORDER BY 1;
