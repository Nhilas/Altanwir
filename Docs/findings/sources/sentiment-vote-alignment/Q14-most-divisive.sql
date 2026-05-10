-- Q14 — Most-divisive games (highest absolute alignment among >=1k-review games, no whitelist)
-- See whatever surfaces — including unknown games. ABS sort.
-- Light: vw_factGameScores is ~30K rows
-- Output: G:/Work/Altanwir-scratch/findings-results/Q14-most-divisive.csv

COPY (
  SELECT
    gameName,
    sentimentVoteAlignment,
    ABS(sentimentVoteAlignment) AS abs_alignment,
    totalReviews,
    weightedSentimentRating,
    weightedVoteRating,
    steamRatingLabel,
    avgPlaytimeAtReviewHours,
    pctNegativeSentiment,
    pctBugReports,
    pctEarlyAccess
  FROM gold.vw_factGameScores
  WHERE totalReviews >= 1000
  ORDER BY ABS(sentimentVoteAlignment) DESC
  LIMIT 10
) TO 'G:/Work/Altanwir-scratch/findings-results/Q14-most-divisive.csv' (HEADER, DELIMITER ',');
