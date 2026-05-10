-- Q23 — Top games by weightedSentimentRating (smoothed)
-- Floor: totalReviews >= 10,000 (post-shrinkage top is naturally lifted, but the
-- floor keeps the table to recognisable / audience-tested titles rather than
-- mid-volume indies the smoothing happens to seat near the ceiling).
-- Output: G:/Work/Altanwir-scratch/findings-results/Q23-top-sentimentrated-games.csv

COPY (
  WITH top_games AS (
    SELECT gameKey, gameName,
           weightedSentimentRating, weightedVoteRating, sentimentVoteAlignment,
           totalReviews, sentimentReviews, steamRatingLabel,
           avgPlaytimeAtReviewHours, pctNegativeSentiment, pctRefunded, pctBugReports
    FROM gold.vw_factGameScores
    WHERE totalReviews >= 10000
    ORDER BY weightedSentimentRating DESC
    LIMIT 15
  ),
  catalog AS (
    SELECT gameKey,
           STRING_AGG(DISTINCT NULLIF(genreName, 'Unknown'), ', '
                      ORDER BY NULLIF(genreName, 'Unknown')) AS genres,
           STRING_AGG(DISTINCT NULLIF(themeName, 'Unknown'), ', '
                      ORDER BY NULLIF(themeName, 'Unknown')) AS themes
    FROM gold.vw_gameCatalogue
    WHERE gameKey IN (SELECT gameKey FROM top_games)
    GROUP BY gameKey
  )
  SELECT t.gameName,
         t.weightedSentimentRating, t.weightedVoteRating, t.sentimentVoteAlignment,
         t.totalReviews, t.sentimentReviews, t.steamRatingLabel,
         t.avgPlaytimeAtReviewHours, t.pctNegativeSentiment,
         t.pctRefunded, t.pctBugReports,
         c.genres, c.themes
  FROM top_games t
  LEFT JOIN catalog c USING (gameKey)
  ORDER BY t.weightedSentimentRating DESC
) TO 'G:/Work/Altanwir-scratch/findings-results/Q23-top-sentimentrated-games.csv' (HEADER, DELIMITER ',');
