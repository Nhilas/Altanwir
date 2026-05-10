-- Q10 — Top 10 funniest single reviews (by votesFunny)
-- Heavy: ORDER BY over 70.9M; uses CTE+LIMIT-first pattern from Amendment A9
-- Output: G:/Work/Altanwir-scratch/findings-results/Q10-funniest-reviews.csv

COPY (
  WITH top10 AS (
    SELECT reviewKey, gameKey, votesFunny, votesUp, commentCount,
           reviewLength, wordCount, isVaderEligible, hasCredibleText,
           sentimentCompound, votedUp,
           SUBSTRING(reviewRaw, 1, 200) AS snippet
    FROM silver.steamreviews
    WHERE votesFunny > 0
    ORDER BY votesFunny DESC
    LIMIT 10
  )
  SELECT
    g.gameName,
    t.votesFunny,
    t.votesUp,
    t.commentCount,
    t.reviewLength,
    t.wordCount,
    t.votedUp,
    CASE
      WHEN t.sentimentCompound IS NULL THEN NULL
      WHEN t.sentimentCompound >= 0.05 THEN 'Positive'
      WHEN t.sentimentCompound <= -0.05 THEN 'Negative'
      ELSE 'Neutral'
    END AS sentimentLabel,
    t.snippet
  FROM top10 t
  LEFT JOIN silver.games g ON g.gameKey = t.gameKey
  ORDER BY t.votesFunny DESC
) TO 'G:/Work/Altanwir-scratch/findings-results/Q10-funniest-reviews.csv' (HEADER, DELIMITER ',');
