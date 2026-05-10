-- Q23 — Funniest reviews per "gaming-disaster" game from Q17b
-- For each of the top-density games (Fast & Furious: Crossroads, Culling II,
-- Day One: Garry's Incident, LOTR: Gollum, Hunt Down the Freeman, BrickForce, Gasp,
-- The Forgotten Ones), pull the top 3 reviews by votesFunny. The snippets surface
-- *what* the joke is about (the disaster shows up in the prose).
-- Output: G:/Work/Altanwir-scratch/findings-results/Q23-disaster-snippets.csv

COPY (
  WITH targets AS (
    SELECT gameKey, gameName
    FROM silver.games
    WHERE gameName IN (
      'Fast & Furious: Crossroads',
      'The Culling II',
      'Day One: Garry''s Incident',
      'The Lord of the Rings: Gollum',
      'Hunt Down the Freeman',
      'BrickForce',
      'Gasp',
      'The Forgotten Ones'
    )
  ),
  ranked AS (
    SELECT
      r.gameKey,
      r.votesFunny,
      r.votesUp,
      r.votedUp,
      r.reviewLength,
      r.wordCount,
      CASE
        WHEN r.sentimentCompound IS NULL THEN NULL
        WHEN r.sentimentCompound >= 0.05 THEN 'Positive'
        WHEN r.sentimentCompound <= -0.05 THEN 'Negative'
        ELSE 'Neutral'
      END AS sentimentLabel,
      SUBSTRING(r.reviewRaw, 1, 280) AS snippet,
      ROW_NUMBER() OVER (PARTITION BY r.gameKey ORDER BY r.votesFunny DESC) AS rn
    FROM silver.steamreviews r
    WHERE r.gameKey IN (SELECT gameKey FROM targets)
      AND r.votesFunny > 0
  )
  SELECT
    t.gameName,
    rk.votesFunny,
    rk.votesUp,
    rk.votedUp,
    rk.sentimentLabel,
    rk.snippet
  FROM ranked rk
  JOIN targets t ON t.gameKey = rk.gameKey
  WHERE rk.rn <= 3
  ORDER BY t.gameName, rk.votesFunny DESC
) TO 'G:/Work/Altanwir-scratch/findings-results/Q23-disaster-snippets.csv' (HEADER, DELIMITER ',');
