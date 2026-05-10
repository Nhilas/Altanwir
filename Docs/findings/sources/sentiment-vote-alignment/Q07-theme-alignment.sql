-- Q07 — Theme-grain alignment (negative tail)
-- v2 refactor (Amendment 1 A8): add avgPlaytimeAtReviewHours for context
-- Output: G:/Work/Altanwir-scratch/findings-results/Q07-theme-alignment.csv

COPY (
  SELECT
    themeName,
    ratedGames,
    reviewedGames,
    totalReviews,
    sentimentVoteAlignment,
    weightedSentimentRating,
    weightedVoteRating,
    pctNegativeSentiment,
    avgPlaytimeAtReviewHours
  FROM gold.vw_aggThemes
  WHERE ratedGames >= 100
  ORDER BY sentimentVoteAlignment ASC
  LIMIT 10
) TO 'G:/Work/Altanwir-scratch/findings-results/Q07-theme-alignment.csv' (HEADER, DELIMITER ',');
