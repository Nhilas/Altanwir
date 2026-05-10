-- Q24 — Top 25 longest-playtime reviews (extension of Q12 from 10 to 25)
-- Same pattern as Q12; surfaces more of the playtime tail for funny.md
-- Output: G:/Work/Altanwir-scratch/findings-results/Q24-playtime-tail-extended.csv

COPY (
  WITH top25 AS (
    SELECT reviewKey, gameKey, playtimeAtReview, playtimeForever,
           votesUp, votesFunny, commentCount,
           reviewLength, wordCount, isVaderEligible,
           sentimentCompound, votedUp,
           SUBSTRING(reviewRaw, 1, 220) AS snippet
    FROM silver.steamreviews
    WHERE playtimeAtReview IS NOT NULL
    ORDER BY playtimeAtReview DESC
    LIMIT 25
  )
  SELECT
    g.gameName,
    ROUND(t.playtimeAtReview / 60.0, 1) AS hours_at_review,
    ROUND(t.playtimeForever / 60.0, 1) AS hours_total,
    t.votesUp,
    t.votesFunny,
    t.votedUp,
    CASE
      WHEN t.sentimentCompound IS NULL THEN NULL
      WHEN t.sentimentCompound >= 0.05 THEN 'Positive'
      WHEN t.sentimentCompound <= -0.05 THEN 'Negative'
      ELSE 'Neutral'
    END AS sentimentLabel,
    t.snippet
  FROM top25 t
  LEFT JOIN silver.games g ON g.gameKey = t.gameKey
  ORDER BY t.playtimeAtReview DESC
) TO 'G:/Work/Altanwir-scratch/findings-results/Q24-playtime-tail-extended.csv' (HEADER, DELIMITER ',');
