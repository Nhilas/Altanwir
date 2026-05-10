-- Q30 — Per-game top review drill-down for the sentiment-vote-alignment tails.
-- Picks: 3 from the negative tail (one per cluster + the survival-horror flagship)
--   - Doom (punishing-difficulty cluster, -21.16)
--   - Sekiro: Shadows Die Twice (punishing-difficulty / Souls-like, -17.23)
--   - Phasmophobia (survival-horror sandbox, -16.27)
-- Picks: 3 from the positive tail (disappointment-AAA cluster)
--   - Starfield (+16.64)
--   - Borderlands 4 (+11.87)
--   - ARK: Survival Ascended (+10.67)
-- Returns: review weight signals + cleaned review text for the single highest-influence review per game.
-- Performance: filters factreviews directly via WHERE gameKey IN (...) per the standing rule against
-- chained scans through vw_gameCatalogue → vw_factGameScores → factreviews.
-- Output: G:/Work/Altanwir-scratch/findings-results/Q30-alignment-drilldown.csv

COPY (
  -- gameName is not unique on Steam (multiple "Doom" entries exist as separate appIds).
  -- Disambiguate with totalReviews so each pick lands on the exact franchise entry shown
  -- in Q01-alignment-negative / Q02-alignment-positive.
  WITH picks(gameName, totalReviews) AS (VALUES
    ('Doom',                        117016),
    ('Sekiro: Shadows Die Twice',    97778),
    ('Phasmophobia',                426001),
    ('Starfield',                   115564),
    ('Borderlands 4',                54350),
    ('Ark: Survival Ascended',       57523)
  ),
  pick_keys AS (
    SELECT fgs.gameKey, fgs.gameName, fgs.sentimentVoteAlignment
    FROM gold.vw_factGameScores fgs
    JOIN picks p ON p.gameName = fgs.gameName AND p.totalReviews = fgs.totalReviews
  ),
  top_reviews AS (
    SELECT
      r.gameKey,
      r.reviewInfluenceScore,
      r.votesUp,
      r.votesFunny,
      r.commentCount,
      r.votedUp,
      ROUND(r.playtimeAtReview / 60.0, 1) AS playtimeHours,
      r.sentimentCompound,
      r.reviewCleaned,
      ROW_NUMBER() OVER (PARTITION BY r.gameKey ORDER BY r.reviewInfluenceScore DESC) AS revRank
    FROM gold.factreviews r
    WHERE r.gameKey IN (SELECT gameKey FROM pick_keys)
      AND r.isVaderEligible = TRUE
  )
  SELECT
    g.gameName,
    g.sentimentVoteAlignment,
    r.reviewInfluenceScore,
    r.votesUp,
    r.votesFunny,
    r.commentCount,
    r.votedUp,
    r.playtimeHours,
    r.sentimentCompound,
    r.reviewCleaned
  FROM top_reviews r
  JOIN pick_keys g USING (gameKey)
  WHERE r.revRank = 1
  ORDER BY g.sentimentVoteAlignment DESC
) TO 'G:/Work/Altanwir-scratch/findings-results/Q30-alignment-drilldown.csv' (HEADER, DELIMITER ',');
