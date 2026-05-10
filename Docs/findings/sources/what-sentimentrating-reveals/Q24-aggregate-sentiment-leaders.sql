-- Q24 — Theme + genre leaders by weightedSentimentRating (smoothed)
-- Floor: ratedGames >= 100 to drop tiny dim members.
-- Two CSV outputs (themes + genres), parallel column shape.
-- Outputs:
--   G:/Work/Altanwir-scratch/findings-results/Q24-theme-sentiment-leaders.csv
--   G:/Work/Altanwir-scratch/findings-results/Q24-genre-sentiment-leaders.csv

COPY (
  SELECT themeName,
         ratedGames, reviewedGames, totalReviews, sentimentReviews,
         weightedSentimentRating, weightedVoteRating, sentimentVoteAlignment,
         pctNegativeSentiment, avgPlaytimeAtReviewHours,
         pctRefunded, pctBugReports
  FROM gold.vw_aggThemes
  WHERE ratedGames >= 100
  ORDER BY weightedSentimentRating DESC
  LIMIT 10
) TO 'G:/Work/Altanwir-scratch/findings-results/Q24-theme-sentiment-leaders.csv' (HEADER, DELIMITER ',');

COPY (
  SELECT genreName,
         ratedGames, reviewedGames, totalReviews, sentimentReviews,
         weightedSentimentRating, weightedVoteRating, sentimentVoteAlignment,
         pctNegativeSentiment, avgPlaytimeAtReviewHours,
         pctRefunded, pctBugReports
  FROM gold.vw_aggGenres
  WHERE ratedGames >= 100
  ORDER BY weightedSentimentRating DESC
  LIMIT 10
) TO 'G:/Work/Altanwir-scratch/findings-results/Q24-genre-sentiment-leaders.csv' (HEADER, DELIMITER ',');
