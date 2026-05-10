-- Q12 — Top 10 longest-playtime reviews (playtimeAtReview)
-- Heavy: ORDER BY over 70.9M; CTE+LIMIT-first
-- Output: G:/Work/Altanwir-scratch/findings-results/Q12-longest-playtime.csv

COPY (
  WITH top10 AS (
    SELECT reviewKey, gameKey, playtimeAtReview, playtimeForever,
           votesUp, votesFunny, commentCount,
           reviewLength, wordCount, isVaderEligible, hasCredibleText,
           sentimentCompound, votedUp,
           SUBSTRING(reviewRaw, 1, 150) AS snippet
    FROM silver.steamreviews
    WHERE playtimeAtReview IS NOT NULL
    ORDER BY playtimeAtReview DESC
    LIMIT 10
  )
  SELECT
    g.gameName,
    ROUND(t.playtimeAtReview / 60.0, 1) AS hours_at_review,
    ROUND(t.playtimeForever / 60.0, 1) AS hours_total,
    t.votesUp,
    t.votesFunny,
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
  ORDER BY t.playtimeAtReview DESC
) TO 'G:/Work/Altanwir-scratch/findings-results/Q12-longest-playtime.csv' (HEADER, DELIMITER ',');
