-- Q27 — Genre drill-down: top 3 games per genre (Point-and-click/Puzzle/Platform)
-- For each game: top review by reviewInfluenceScore, flattened allGenres, key weight fields + snippet.
-- Output: G:/Work/Altanwir-scratch/findings-results/Q27-genre-game-drilldown.csv

COPY (
  WITH target_genres AS (
    SELECT DISTINCT genreName, gameKey
    FROM gold.vw_gameGenres
    WHERE genreName IN ('Point-and-click', 'Puzzle', 'Platform')
  ),
  game_scores AS (
    SELECT tg.genreName, tg.gameKey, gs.gameName,
           gs.weightedSentimentRating, gs.totalReviews
    FROM target_genres tg
    JOIN gold.vw_factGameScores gs USING (gameKey)
  ),
  ranked_games AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY genreName ORDER BY weightedSentimentRating DESC) AS gameRank
    FROM game_scores
  ),
  top3_games AS (
    SELECT genreName, gameKey, gameName, weightedSentimentRating, totalReviews
    FROM ranked_games
    WHERE gameRank <= 3
  ),
  all_genres_per_game AS (
    SELECT gc.gameKey,
           STRING_AGG(DISTINCT NULLIF(gc.genreName, 'Unknown'), ', '
                      ORDER BY NULLIF(gc.genreName, 'Unknown')) AS allGenres
    FROM gold.vw_gameGenres gc
    WHERE gc.gameKey IN (SELECT gameKey FROM top3_games)
    GROUP BY gc.gameKey
  ),
  game_keys AS (
    SELECT gameKey FROM top3_games
  ),
  top_reviews AS (
    SELECT
      r.gameKey,
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
    t.genreName,
    t.gameName,
    t.weightedSentimentRating,
    t.totalReviews,
    a.allGenres,
    r.reviewInfluenceScore,
    r.votesUp,
    r.votesFunny,
    r.commentCount,
    r.reactionCount,
    r.playtimeHours,
    r.wordCount,
    r.reviewCleaned
  FROM top3_games t
  LEFT JOIN all_genres_per_game a USING (gameKey)
  LEFT JOIN best_review r USING (gameKey)
  ORDER BY t.genreName, t.weightedSentimentRating DESC
) TO 'G:/Work/Altanwir-scratch/findings-results/Q27-genre-game-drilldown.csv' (HEADER, DELIMITER ',');
