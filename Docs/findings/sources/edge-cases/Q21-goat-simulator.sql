-- Q21 — Goat Simulator literal goats (the meta-joke)
-- Does Goat Simulator actually have GoatGoatGoat... reviews?
-- Heavy: ORDER BY over 70.9M, but with restrictive WHERE; should be fast via parquet pushdown
-- Output: G:/Work/Altanwir-scratch/findings-results/Q21-goat-simulator.csv

COPY (
  WITH goat_games AS (
    SELECT gameKey, gameName
    FROM silver.games
    WHERE gameName ILIKE '%Goat%'
  )
  SELECT
    gg.gameName,
    s.reviewLength,
    s.wordCount,
    s.wordLengthRatio,
    s.hasCredibleText,
    s.isVaderEligible,
    s.votesUp,
    s.votesFunny,
    SUBSTRING(s.reviewRaw, 1, 200) AS snippet
  FROM silver.steamreviews s
  JOIN goat_games gg ON gg.gameKey = s.gameKey
  WHERE s.wordCount = 1 AND s.reviewLength > 1000
  ORDER BY s.reviewLength DESC, s.votesUp DESC
  LIMIT 10
) TO 'G:/Work/Altanwir-scratch/findings-results/Q21-goat-simulator.csv' (HEADER, DELIMITER ',');
