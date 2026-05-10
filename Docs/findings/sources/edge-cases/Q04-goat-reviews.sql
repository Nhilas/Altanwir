-- Q04 — Goat-review anecdote: long single-token reviews caught by hasCredibleText
-- v2 refactor (Amendment 1): add gameName via JOIN to silver.games (so we can see WHICH games attract Goats)
-- Output: G:/Work/Altanwir-scratch/findings-results/Q04-goat-reviews.csv

COPY (
  WITH top10 AS (
    SELECT reviewKey, gameKey, reviewLength, wordCount, wordLengthRatio, asciiRatio,
           uniqueWordRatio, hasCredibleText, isVaderEligible, reviewRaw
    FROM silver.steamreviews
    WHERE wordCount = 1 AND reviewLength > 5000
    ORDER BY reviewLength DESC
    LIMIT 10
  )
  SELECT
    g.gameName,
    t.reviewLength,
    t.wordCount,
    t.wordLengthRatio,
    t.asciiRatio,
    t.uniqueWordRatio,
    t.hasCredibleText,
    t.isVaderEligible,
    SUBSTRING(t.reviewRaw, 1, 200) AS snippet,
    t.reviewKey
  FROM top10 t
  LEFT JOIN silver.games g ON g.gameKey = t.gameKey
  ORDER BY t.reviewLength DESC
) TO 'G:/Work/Altanwir-scratch/findings-results/Q04-goat-reviews.csv' (HEADER, DELIMITER ',');
