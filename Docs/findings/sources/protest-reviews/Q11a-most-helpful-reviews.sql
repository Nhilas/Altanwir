-- Q11a — Top 10 most-helpful single reviews (by votesUp)
-- Heavy: ORDER BY over 70.9M; CTE+LIMIT-first
-- Output: G:/Work/Altanwir-scratch/findings-results/Q11a-most-helpful-reviews.csv

COPY (
  WITH top10 AS (
    SELECT reviewKey, gameKey, votesUp, votesFunny, commentCount,
           reviewLength, wordCount, isVaderEligible, hasCredibleText,
           sentimentCompound, votedUp, playtimeAtReview,
           SUBSTRING(reviewRaw, 1, 200) AS snippet
    FROM silver.steamreviews
    ORDER BY votesUp DESC
    LIMIT 10
  )
  SELECT
    g.gameName,
    t.votesUp,
    t.votesFunny,
    t.commentCount,
    t.reviewLength,
    t.wordCount,
    t.votedUp,
    ROUND(t.playtimeAtReview / 60.0, 1) AS hours_at_review,
    CASE
      WHEN t.sentimentCompound IS NULL THEN NULL
      WHEN t.sentimentCompound >= 0.05 THEN 'Positive'
      WHEN t.sentimentCompound <= -0.05 THEN 'Negative'
      ELSE 'Neutral'
    END AS sentimentLabel,
    t.snippet
  FROM top10 t
  LEFT JOIN silver.games g ON g.gameKey = t.gameKey
  ORDER BY t.votesUp DESC
) TO 'G:/Work/Altanwir-scratch/findings-results/Q11a-most-helpful-reviews.csv' (HEADER, DELIMITER ',');
