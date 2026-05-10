-- Q25 — Funniest review per IGDB theme (curated theme list)
-- For each theme, pull the top 2 reviews by votesFunny across games tagged with that theme.
-- Surfaces theme-specific review culture (Erotic dating-sims, Comedy meme reviews, Horror nope-noped).
-- vw_gameCatalogue is cartesian (gameKey × theme × genre × platform); dedup to (gameKey, themeName) before joining reviews.
-- Output: G:/Work/Altanwir-scratch/findings-results/Q25-funny-by-theme.csv

COPY (
  WITH theme_filter AS (
    SELECT DISTINCT gameKey, themeName
    FROM gold.vw_gameCatalogue
    WHERE themeName IN ('Comedy', 'Erotic', 'Horror', 'Stealth', 'Historical', 'Drama', 'Educational', 'Sandbox')
  ),
  ranked AS (
    SELECT
      tf.themeName,
      r.gameKey,
      r.votesFunny,
      r.votesUp,
      r.votedUp,
      CASE
        WHEN r.sentimentCompound IS NULL THEN NULL
        WHEN r.sentimentCompound >= 0.05 THEN 'Positive'
        WHEN r.sentimentCompound <= -0.05 THEN 'Negative'
        ELSE 'Neutral'
      END AS sentimentLabel,
      SUBSTRING(r.reviewRaw, 1, 280) AS snippet,
      ROW_NUMBER() OVER (PARTITION BY tf.themeName ORDER BY r.votesFunny DESC) AS rn
    FROM silver.steamreviews r
    JOIN theme_filter tf ON tf.gameKey = r.gameKey
    WHERE r.votesFunny > 0
  )
  SELECT
    rk.themeName,
    g.gameName,
    rk.votesFunny,
    rk.votesUp,
    rk.votedUp,
    rk.sentimentLabel,
    rk.snippet
  FROM ranked rk
  JOIN silver.games g ON g.gameKey = rk.gameKey
  WHERE rk.rn <= 2
  ORDER BY rk.themeName, rk.votesFunny DESC
) TO 'G:/Work/Altanwir-scratch/findings-results/Q25-funny-by-theme.csv' (HEADER, DELIMITER ',');
