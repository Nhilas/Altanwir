-- Q17b — Funny-density (vs Q17 raw-volume sort): high funny-per-review games
-- Surfaces meme-density: Day One: Garry's Incident (38.84/review), Hunt Down the Freeman (12.09/review)
-- Sub-agent suggested addition for Batch 2
-- Output: G:/Work/Altanwir-scratch/findings-results/Q17b-funny-density.csv

COPY (
  WITH funny_agg AS (
    SELECT
      gameKey,
      SUM(votesFunny)::BIGINT AS total_funny,
      COUNT(*)::BIGINT AS total_reviews
    FROM silver.steamreviews
    GROUP BY gameKey
    HAVING SUM(votesFunny) >= 5000
  )
  SELECT
    g.gameName,
    fa.total_funny,
    fa.total_reviews,
    ROUND(fa.total_funny::DOUBLE / fa.total_reviews, 2) AS funny_per_review,
    fgs.pctNegativeSentiment,
    fgs.weightedSentimentRating,
    fgs.weightedVoteRating,
    fgs.sentimentVoteAlignment,
    fgs.steamRatingLabel
  FROM funny_agg fa
  LEFT JOIN silver.games g ON g.gameKey = fa.gameKey
  LEFT JOIN gold.vw_factGameScores fgs ON fgs.gameKey = fa.gameKey
  WHERE fgs.pctNegativeSentiment >= 30
  ORDER BY (fa.total_funny::DOUBLE / fa.total_reviews) DESC
  LIMIT 10
) TO 'G:/Work/Altanwir-scratch/findings-results/Q17b-funny-density.csv' (HEADER, DELIMITER ',');
