-- Q25 — Per-game top review drill-down for the Q23 top-3 games
-- (A Short Hike, Fields of Mistria, Tiny Glade — the top 3 by weightedSentimentRating >= 10k reviews)
-- Returns: review weight signals + cleaned review text for the single highest-influence review per game.
-- Output: G:/Work/Altanwir-scratch/findings-results/Q25-game-review-drilldown.csv

COPY (
  WITH top3_game_names AS (
    SELECT gameKey, gameName, weightedSentimentRating
    FROM gold.vw_factGameScores
    WHERE totalReviews >= 10000
    ORDER BY weightedSentimentRating DESC
    LIMIT 3
  ),
  top_reviews AS (
    SELECT
      r.gameKey,
      r.reviewInfluenceScore,
      r.votesUp,
      r.votesFunny,
      r.commentCount,
      r.reactionCount,
      r.communitySignal,
      ROUND(r.playtimeAtReview / 60.0, 1)  AS playtimeHours,
      r.playtimeSignal,
      r.wordCount,
      r.wordLengthRatio,
      r.lengthSignal,
      r.sentimentCompound,
      r.sentimentSignal,
      r.emotionalIntensity,
      r.emotionalSignal,
      r.reviewCleaned,
      ROW_NUMBER() OVER (PARTITION BY r.gameKey ORDER BY r.reviewInfluenceScore DESC) AS revRank
    FROM gold.factreviews r
    WHERE r.gameKey IN (SELECT gameKey FROM top3_game_names)
      AND r.isVaderEligible = TRUE
  )
  SELECT
    g.gameName,
    g.weightedSentimentRating,
    r.reviewInfluenceScore,
    r.votesUp,
    r.votesFunny,
    r.commentCount,
    r.reactionCount,
    r.communitySignal,
    r.playtimeHours,
    r.playtimeSignal,
    r.wordCount,
    r.wordLengthRatio,
    r.lengthSignal,
    r.sentimentCompound,
    r.sentimentSignal,
    r.emotionalIntensity,
    r.emotionalSignal,
    r.reviewCleaned
  FROM top_reviews r
  JOIN top3_game_names g USING (gameKey)
  WHERE r.revRank = 1
  ORDER BY g.weightedSentimentRating DESC
) TO 'G:/Work/Altanwir-scratch/findings-results/Q25-game-review-drilldown.csv' (HEADER, DELIMITER ',');
