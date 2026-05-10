-- Q28 — Golden zone: top genre×theme combos by weighted sentiment rating
-- Top 3 combos (min 50 games), top 3 games per combo, top review snippet per game.
-- Outputs:
--   G:/Work/Altanwir-scratch/findings-results/Q28-golden-zone-combos.csv
--   G:/Work/Altanwir-scratch/findings-results/Q28-golden-zone-games.csv

-- Step 1: top 3 combos
COPY (
  WITH combo_scores AS (
    SELECT
      gc.genreName,
      gc.themeName,
      COUNT(DISTINCT gc.gameKey)                                         AS comboGames,
      SUM(gs.weightedSentimentRating * gs.sentimentReviews)
        / NULLIF(SUM(gs.sentimentReviews), 0)                           AS comboSentiment
    FROM gold.vw_gameCatalogue gc
    JOIN gold.vw_factGameScores gs USING (gameKey)
    WHERE gc.genreName NOT IN ('Unknown') AND gc.themeName NOT IN ('Unknown')
    GROUP BY gc.genreName, gc.themeName
    HAVING COUNT(DISTINCT gc.gameKey) >= 50
  )
  SELECT genreName, themeName, comboGames,
         ROUND(comboSentiment, 2) AS comboSentimentRating
  FROM combo_scores
  ORDER BY comboSentiment DESC
  LIMIT 3
) TO 'G:/Work/Altanwir-scratch/findings-results/Q28-golden-zone-combos.csv' (HEADER, DELIMITER ',');

-- Step 2: top 3 games per combo + top review snippet
COPY (
  WITH top_combos AS (
    SELECT genreName, themeName
    FROM (
      SELECT
        gc.genreName,
        gc.themeName,
        SUM(gs.weightedSentimentRating * gs.sentimentReviews)
          / NULLIF(SUM(gs.sentimentReviews), 0) AS comboSentiment,
        COUNT(DISTINCT gc.gameKey)               AS comboGames
      FROM gold.vw_gameCatalogue gc
      JOIN gold.vw_factGameScores gs USING (gameKey)
      WHERE gc.genreName NOT IN ('Unknown') AND gc.themeName NOT IN ('Unknown')
      GROUP BY gc.genreName, gc.themeName
      HAVING COUNT(DISTINCT gc.gameKey) >= 50
      ORDER BY comboSentiment DESC
      LIMIT 3
    ) t
  ),
  combo_games_raw AS (
    -- vw_gameCatalogue is a cartesian game×genre×theme; DISTINCT dedups before ranking.
    SELECT DISTINCT
      tc.genreName,
      tc.themeName,
      gc.gameKey,
      gs.gameName,
      gs.weightedSentimentRating,
      gs.totalReviews
    FROM top_combos tc
    JOIN gold.vw_gameCatalogue gc
      ON gc.genreName = tc.genreName AND gc.themeName = tc.themeName
    JOIN gold.vw_factGameScores gs USING (gameKey)
  ),
  combo_games AS (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY genreName, themeName
        ORDER BY weightedSentimentRating DESC
      ) AS gameRank
    FROM combo_games_raw
  ),
  top3_combo_games AS (
    SELECT genreName, themeName, gameKey, gameName,
           weightedSentimentRating, totalReviews
    FROM combo_games
    WHERE gameRank <= 3
  ),
  all_themes_per_game AS (
    SELECT gc2.gameKey,
           STRING_AGG(DISTINCT NULLIF(gc2.themeName, 'Unknown'), ', '
                      ORDER BY NULLIF(gc2.themeName, 'Unknown')) AS allThemes
    FROM gold.vw_gameThemes gc2
    WHERE gc2.gameKey IN (SELECT gameKey FROM top3_combo_games)
    GROUP BY gc2.gameKey
  ),
  all_genres_per_game AS (
    SELECT gc3.gameKey,
           STRING_AGG(DISTINCT NULLIF(gc3.genreName, 'Unknown'), ', '
                      ORDER BY NULLIF(gc3.genreName, 'Unknown')) AS allGenres
    FROM gold.vw_gameGenres gc3
    WHERE gc3.gameKey IN (SELECT gameKey FROM top3_combo_games)
    GROUP BY gc3.gameKey
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
    WHERE r.gameKey IN (SELECT gameKey FROM top3_combo_games)
      AND r.isVaderEligible = TRUE
  ),
  best_review AS (
    SELECT gameKey, reviewInfluenceScore, votesUp, votesFunny,
           commentCount, reactionCount, playtimeHours, wordCount, reviewCleaned
    FROM top_reviews WHERE revRank = 1
  )
  SELECT
    t.genreName,
    t.themeName,
    t.gameName,
    t.weightedSentimentRating,
    t.totalReviews,
    a.allThemes,
    g.allGenres,
    r.reviewInfluenceScore,
    r.votesUp,
    r.votesFunny,
    r.commentCount,
    r.reactionCount,
    r.playtimeHours,
    r.wordCount,
    r.reviewCleaned
  FROM top3_combo_games t
  LEFT JOIN all_themes_per_game a USING (gameKey)
  LEFT JOIN all_genres_per_game g USING (gameKey)
  LEFT JOIN best_review r USING (gameKey)
  ORDER BY t.genreName, t.themeName, t.weightedSentimentRating DESC
) TO 'G:/Work/Altanwir-scratch/findings-results/Q28-golden-zone-games.csv' (HEADER, DELIMITER ',');
