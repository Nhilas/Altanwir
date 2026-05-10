-- Q20 — Highest refund-rate games (among >=10k-review games)
-- Light: vw_factGameScores
-- Output: G:/Work/Altanwir-scratch/findings-results/Q20-refund-rates.csv

COPY (
  SELECT
    gameName,
    pctRefunded,
    totalReviews,
    weightedSentimentRating,
    weightedVoteRating,
    sentimentVoteAlignment,
    steamRatingLabel,
    pctBugReports,
    pctEarlyAccess
  FROM gold.vw_factGameScores
  WHERE totalReviews >= 10000
  ORDER BY pctRefunded DESC
  LIMIT 10
) TO 'G:/Work/Altanwir-scratch/findings-results/Q20-refund-rates.csv' (HEADER, DELIMITER ',');
