-- Q32 — Tier-mismatch drill-down: 3 recognizable 100k+ games per pattern
-- Patterns: same tier, vote tier higher, sentiment tier higher.
-- Picks chosen to avoid duplication with sentiment-vote-alignment.md's tables.
-- Output: G:/Work/Altanwir-scratch/findings-results/Q32-tier-mismatch-drilldown.csv

WITH game_set(pattern, pat_ord, row_ord, gameKey) AS (
  VALUES
    ('A. same tier',          1, 1, 'c232ddce533bb1140c6298d66158cb03f91d925bff0c9a2a1a80c9b57954ac7e'), -- Skyrim SE
    ('A. same tier',          1, 2, '19803b033158701ce0e2edab5dde9719cfcc5eb7d47bce17db0a79c2c14c09a6'), -- Rocket League
    ('A. same tier',          1, 3, 'e4e467d7eed779cfd7a3c5bd39d25d7f9ef3580738a184378cd193e0b06841d7'), -- Civ VI
    ('B. vote tier higher',   2, 1, 'ed319374d28d2fc9ba792fea0e00b58286dd48f0907fe72032d50c926f45dea1'), -- Helldivers 2
    ('B. vote tier higher',   2, 2, '6affdae3b3c1aa6aa7689e9b6a7b3225a636aa1ac0025f490cca1285ceaf1487'), -- Left 4 Dead 2
    ('B. vote tier higher',   2, 3, '175bfb2b0cc8930c08df934e84602e296183596ffaa897d16fc86e3b68e91b9d'), -- The Binding of Isaac: Rebirth
    ('C. sentiment tier higher', 3, 1, 'a37c1f17e3f5942ae76094001a3ee2d946b6d1b41ba9d54c8dc2d96150ed7f3c'), -- New World: Aeternum
    ('C. sentiment tier higher', 3, 2, '5cf5d7e5a49b01defab8f656bc073464da8713defb5fdcc59f77434799871a0d'), -- Halo Infinite
    ('C. sentiment tier higher', 3, 3, 'cf275d22796dc485dcff2a9672ae8db6b2a93bcabfb3576ae46e8ebaf3604a9d')  -- Black Desert
),
games AS (
  SELECT gs.pattern, gs.pat_ord, gs.row_ord,
         f.gameKey, f.gameName, f.totalReviews,
         f.weightedSentimentRating, f.weightedSentimentTier,
         f.weightedVoteRating, f.weightedVoteTier,
         f.sentimentVoteAlignment,
         f.pctBugReports, f.pctEarlyAccess, f.pctRefunded
  FROM game_set gs
  JOIN gold.vw_factGameScores f USING (gameKey)
),
themes_dedup AS (
  SELECT DISTINCT gameKey, themeName
  FROM gold.vw_gameCatalogue
  WHERE themeName <> 'Unknown' AND themeName IS NOT NULL
),
themes_agg AS (
  SELECT gameKey, STRING_AGG(themeName, ', ' ORDER BY themeName) AS themes
  FROM themes_dedup
  GROUP BY gameKey
),
genres_dedup AS (
  SELECT DISTINCT gameKey, genreName
  FROM gold.vw_gameCatalogue
  WHERE genreName <> 'Unknown' AND genreName IS NOT NULL
),
genres_agg AS (
  SELECT gameKey, STRING_AGG(genreName, ', ' ORDER BY genreName) AS genres
  FROM genres_dedup
  GROUP BY gameKey
)
SELECT g.pattern,
       g.gameName,
       g.totalReviews,
       ROUND(g.weightedSentimentRating, 2) AS sentiment,
       g.weightedSentimentTier AS s_tier,
       ROUND(g.weightedVoteRating, 2) AS vote,
       g.weightedVoteTier AS v_tier,
       ROUND(g.sentimentVoteAlignment, 2) AS gap,
       ROUND(g.pctBugReports, 2) AS pct_bug_mentions,
       ROUND(g.pctEarlyAccess, 1) AS pct_early_access,
       ROUND(g.pctRefunded, 2) AS pct_refunded,
       gn.genres,
       t.themes
FROM games g
LEFT JOIN themes_agg t  ON t.gameKey = g.gameKey
LEFT JOIN genres_agg gn ON gn.gameKey = g.gameKey
ORDER BY g.pat_ord, g.row_ord;

COPY (
  WITH game_set(pattern, pat_ord, row_ord, gameKey) AS (
    VALUES
      ('A. same tier',          1, 1, 'c232ddce533bb1140c6298d66158cb03f91d925bff0c9a2a1a80c9b57954ac7e'),
      ('A. same tier',          1, 2, '19803b033158701ce0e2edab5dde9719cfcc5eb7d47bce17db0a79c2c14c09a6'),
      ('A. same tier',          1, 3, 'e4e467d7eed779cfd7a3c5bd39d25d7f9ef3580738a184378cd193e0b06841d7'),
      ('B. vote tier higher',   2, 1, 'ed319374d28d2fc9ba792fea0e00b58286dd48f0907fe72032d50c926f45dea1'),
      ('B. vote tier higher',   2, 2, '6affdae3b3c1aa6aa7689e9b6a7b3225a636aa1ac0025f490cca1285ceaf1487'),
      ('B. vote tier higher',   2, 3, '175bfb2b0cc8930c08df934e84602e296183596ffaa897d16fc86e3b68e91b9d'),
      ('C. sentiment tier higher', 3, 1, 'a37c1f17e3f5942ae76094001a3ee2d946b6d1b41ba9d54c8dc2d96150ed7f3c'),
      ('C. sentiment tier higher', 3, 2, '5cf5d7e5a49b01defab8f656bc073464da8713defb5fdcc59f77434799871a0d'),
      ('C. sentiment tier higher', 3, 3, 'cf275d22796dc485dcff2a9672ae8db6b2a93bcabfb3576ae46e8ebaf3604a9d')
  ),
  games AS (
    SELECT gs.pattern, gs.pat_ord, gs.row_ord, f.gameKey, f.gameName, f.totalReviews,
           f.weightedSentimentRating, f.weightedSentimentTier,
           f.weightedVoteRating, f.weightedVoteTier, f.sentimentVoteAlignment,
           f.pctBugReports, f.pctEarlyAccess, f.pctRefunded
    FROM game_set gs JOIN gold.vw_factGameScores f USING (gameKey)
  ),
  themes_dedup AS (SELECT DISTINCT gameKey, themeName FROM gold.vw_gameCatalogue WHERE themeName <> 'Unknown' AND themeName IS NOT NULL),
  themes_agg AS (SELECT gameKey, STRING_AGG(themeName, ', ' ORDER BY themeName) AS themes FROM themes_dedup GROUP BY gameKey),
  genres_dedup AS (SELECT DISTINCT gameKey, genreName FROM gold.vw_gameCatalogue WHERE genreName <> 'Unknown' AND genreName IS NOT NULL),
  genres_agg AS (SELECT gameKey, STRING_AGG(genreName, ', ' ORDER BY genreName) AS genres FROM genres_dedup GROUP BY gameKey)
  SELECT g.pattern, g.gameName, g.totalReviews,
         ROUND(g.weightedSentimentRating, 2) AS sentiment, g.weightedSentimentTier AS s_tier,
         ROUND(g.weightedVoteRating, 2) AS vote, g.weightedVoteTier AS v_tier,
         ROUND(g.sentimentVoteAlignment, 2) AS gap,
         ROUND(g.pctBugReports, 2) AS pct_bug_mentions,
         ROUND(g.pctEarlyAccess, 1) AS pct_early_access,
         ROUND(g.pctRefunded, 2) AS pct_refunded,
         gn.genres, t.themes
  FROM games g
  LEFT JOIN themes_agg t  ON t.gameKey = g.gameKey
  LEFT JOIN genres_agg gn ON gn.gameKey = g.gameKey
  ORDER BY g.pat_ord, g.row_ord
) TO 'G:/Work/Altanwir-scratch/findings-results/Q32-tier-mismatch-drilldown.csv' (HEADER, DELIMITER ',');
