-- Q06 — reviewInfluenceScore: top vs bottom (excl. zero)
-- v2 refactor (Amendment 1): full perf rewrite. LIMIT-on-factreviews-first via CTE; JOIN silver/dim only for surviving rows.
--   Reasons: vw_factReviews forces the JOIN over 70.9M rows pre-sort. Pushing LIMIT before JOIN drops cost dramatically.
--   Also: smaller snippets (120 chars), no echo SELECTs.
-- Output: G:/Work/Altanwir-scratch/findings-results/Q06a-influence-top.csv
-- Output: G:/Work/Altanwir-scratch/findings-results/Q06b-influence-bottom.csv

-- Top 10
COPY (
  WITH top10 AS (
    SELECT reviewKey, gameKey, reviewInfluenceScore
    FROM gold.factreviews
    ORDER BY reviewInfluenceScore DESC NULLS LAST
    LIMIT 10
  )
  SELECT
    t.reviewInfluenceScore,
    g.gameName,
    s.reviewLength,
    s.wordCount,
    s.votesUp,
    s.votesFunny,
    s.commentCount,
    s.playtimeAtReview,
    s.isVaderEligible,
    s.hasCredibleText,
    SUBSTRING(s.reviewRaw, 1, 120) AS snippet
  FROM top10 t
  LEFT JOIN silver.steamreviews s ON s.reviewKey = t.reviewKey
  LEFT JOIN silver.games g ON g.gameKey = t.gameKey
  ORDER BY t.reviewInfluenceScore DESC
) TO 'G:/Work/Altanwir-scratch/findings-results/Q06a-influence-top.csv' (HEADER, DELIMITER ',');

-- Bottom 10 (above zero, to skip null/zero noise)
COPY (
  WITH bottom10 AS (
    SELECT reviewKey, gameKey, reviewInfluenceScore
    FROM gold.factreviews
    WHERE reviewInfluenceScore > 0
    ORDER BY reviewInfluenceScore ASC
    LIMIT 10
  )
  SELECT
    t.reviewInfluenceScore,
    g.gameName,
    s.reviewLength,
    s.wordCount,
    s.votesUp,
    s.votesFunny,
    s.commentCount,
    s.playtimeAtReview,
    s.isVaderEligible,
    s.hasCredibleText,
    SUBSTRING(s.reviewRaw, 1, 120) AS snippet
  FROM bottom10 t
  LEFT JOIN silver.steamreviews s ON s.reviewKey = t.reviewKey
  LEFT JOIN silver.games g ON g.gameKey = t.gameKey
  ORDER BY t.reviewInfluenceScore ASC
) TO 'G:/Work/Altanwir-scratch/findings-results/Q06b-influence-bottom.csv' (HEADER, DELIMITER ',');
