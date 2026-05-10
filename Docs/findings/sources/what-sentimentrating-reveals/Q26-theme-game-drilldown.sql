-- Q26 — Theme drill-down: top 3 games per theme (Business/Educational/Romance)
-- For each game: top review by reviewInfluenceScore, flattened allThemes, key weight fields + snippet.
-- Outputs: G:/Work/Altanwir-scratch/findings-results/Q26-theme-game-drilldown.csv

COPY (
  WITH target_themes AS (
    SELECT DISTINCT themeName, gameKey
    FROM gold.vw_gameThemes
    WHERE themeName IN ('Business', 'Educational', 'Romance')
  ),
  game_scores AS (
    SELECT tg.themeName, tg.gameKey, gs.gameName,
           gs.weightedSentimentRating, gs.totalReviews
    FROM target_themes tg
    JOIN gold.vw_factGameScores gs USING (gameKey)
  ),
  ranked_games AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY themeName ORDER BY weightedSentimentRating DESC) AS gameRank
    FROM game_scores
  ),
  top3_games AS (
    SELECT themeName, gameKey, gameName, weightedSentimentRating, totalReviews
    FROM ranked_games
    WHERE gameRank <= 3
  ),
  all_themes_per_game AS (
    SELECT gc.gameKey,
           STRING_AGG(DISTINCT NULLIF(gc.themeName, 'Unknown'), ', '
                      ORDER BY NULLIF(gc.themeName, 'Unknown')) AS allThemes
    FROM gold.vw_gameThemes gc
    WHERE gc.gameKey IN (SELECT gameKey FROM top3_games)
    GROUP BY gc.gameKey
  ),
  game_keys AS (
    SELECT gameKey FROM top3_games
  ),
  top_reviews AS (
    SELECT
      r.gameKey,
      r.reviewKey,
      r.reviewInfluenceScore,
      r.votesUp,
      r.votesFunny,
      r.commentCount,
      r.reactionCount,
      ROUND(r.playtimeAtReview / 60.0, 1) AS playtimeHours,
      r.wordCount,
      r.reviewCleaned,
      ROW_NUMBER() OVER (PARTITION BY r.gameKey ORDER BY r.reviewInfluenceScore DESC) AS revRank
    FROM gold.factreviews r
    WHERE r.gameKey IN (SELECT gameKey FROM game_keys)
      AND r.isVaderEligible = TRUE
  ),
  best_review AS (
    SELECT gameKey, reviewInfluenceScore, votesUp, votesFunny,
           commentCount, reactionCount, playtimeHours, wordCount, reviewCleaned
    FROM top_reviews
    WHERE revRank = 1
  )
  SELECT
    t.themeName,
    t.gameName,
    t.weightedSentimentRating,
    t.totalReviews,
    a.allThemes,
    r.reviewInfluenceScore,
    r.votesUp,
    r.votesFunny,
    r.commentCount,
    r.reactionCount,
    r.playtimeHours,
    r.wordCount,
    r.reviewCleaned
  FROM top3_games t
  LEFT JOIN all_themes_per_game a USING (gameKey)
  LEFT JOIN best_review r USING (gameKey)
  ORDER BY t.themeName, t.weightedSentimentRating DESC
) TO 'G:/Work/Altanwir-scratch/findings-results/Q26-theme-game-drilldown.csv' (HEADER, DELIMITER ',');
