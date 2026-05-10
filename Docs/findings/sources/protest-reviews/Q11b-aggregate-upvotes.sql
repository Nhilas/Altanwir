-- Q11b — Games with highest aggregate upvotes (sum across all reviews)
-- Heavy: aggregate over 70.9M with GROUP BY 30K games
-- Output: G:/Work/Altanwir-scratch/findings-results/Q11b-aggregate-upvotes.csv

COPY (
  WITH agg AS (
    SELECT
      r.gameKey,
      SUM(r.votesUp)::BIGINT AS total_upvotes,
      COUNT(*)::BIGINT AS total_reviews,
      ROUND(AVG(r.votesUp), 2) AS avg_votes_per_review
    FROM silver.steamreviews r
    GROUP BY r.gameKey
  )
  SELECT
    g.gameName,
    a.total_upvotes,
    a.total_reviews,
    a.avg_votes_per_review,
    fgs.weightedSentimentRating,
    fgs.weightedVoteRating,
    fgs.steamRatingLabel
  FROM agg a
  LEFT JOIN silver.games g ON g.gameKey = a.gameKey
  LEFT JOIN gold.vw_factGameScores fgs ON fgs.gameKey = a.gameKey
  ORDER BY a.total_upvotes DESC
  LIMIT 10
) TO 'G:/Work/Altanwir-scratch/findings-results/Q11b-aggregate-upvotes.csv' (HEADER, DELIMITER ',');
