-- Q13 — Most-reviewed games (popularity ranking)
-- Light: vw_factGameScores is ~30K rows
-- Output: G:/Work/Altanwir-scratch/findings-results/Q13-popularity.csv

COPY (
  SELECT
    gameName,
    totalReviews,
    weightedSentimentRating,
    weightedVoteRating,
    sentimentVoteAlignment,
    steamRatingLabel,
    avgPlaytimeAtReviewHours,
    pctBugReports,
    pctEarlyAccess
  FROM gold.vw_factGameScores
  WHERE totalReviews IS NOT NULL
  ORDER BY totalReviews DESC
  LIMIT 10
) TO 'G:/Work/Altanwir-scratch/findings-results/Q13-popularity.csv' (HEADER, DELIMITER ',');
