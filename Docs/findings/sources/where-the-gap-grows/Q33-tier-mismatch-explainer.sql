-- Q33 — Tier-mismatch explainer: one recognizable 100k+ game per pattern
-- Used as the intro teaser table in Docs/findings/where-the-gap-grows.md.
-- Picks chosen to AVOID overlap with Q32 drill-down and with sentiment-vote-alignment.md tables
-- (Battlefield 2042 also appears in alignment.md as a positive-alignment row; here it is read
-- through the tier-mismatch frame as the lone D->F shift in the 100k+ bucket).
-- Output: G:/Work/Altanwir-scratch/findings-results/Q33-tier-mismatch-explainer.csv

WITH game_set(pattern, ord, gameKey) AS (
  VALUES
    ('A. same tier',             1, '9b275340b1a980f5d7e5dc956d1499580cbe9ba246fbe4fb7de312213020e72c'), -- Cities: Skylines (A/A)
    ('B. vote tier higher',      2, '6a92cc958cd99350a10cc913892554da4e835e36ded7cf30b064a92f27ff972e'), -- People Playground (B->S)
    ('C. sentiment tier higher', 3, '84aa4a6603daa17937127b434db48a2146a7a99b555b66a6e0a078b27c0711e6')  -- Battlefield 2042 (D->F)
)
SELECT gs.pattern,
       f.gameName,
       f.totalReviews,
       ROUND(f.weightedSentimentRating, 2) AS sentiment,
       f.weightedSentimentTier AS s_tier,
       ROUND(f.weightedVoteRating, 2) AS vote,
       f.weightedVoteTier AS v_tier,
       ROUND(f.sentimentVoteAlignment, 2) AS gap
FROM game_set gs
JOIN gold.vw_factGameScores f USING (gameKey)
ORDER BY gs.ord;

COPY (
  WITH game_set(pattern, ord, gameKey) AS (
    VALUES
      ('A. same tier',             1, '9b275340b1a980f5d7e5dc956d1499580cbe9ba246fbe4fb7de312213020e72c'),
      ('B. vote tier higher',      2, '6a92cc958cd99350a10cc913892554da4e835e36ded7cf30b064a92f27ff972e'),
      ('C. sentiment tier higher', 3, '84aa4a6603daa17937127b434db48a2146a7a99b555b66a6e0a078b27c0711e6')
  )
  SELECT gs.pattern, f.gameName, f.totalReviews,
         ROUND(f.weightedSentimentRating, 2) AS sentiment, f.weightedSentimentTier AS s_tier,
         ROUND(f.weightedVoteRating, 2) AS vote, f.weightedVoteTier AS v_tier,
         ROUND(f.sentimentVoteAlignment, 2) AS gap
  FROM game_set gs
  JOIN gold.vw_factGameScores f USING (gameKey)
  ORDER BY gs.ord
) TO 'G:/Work/Altanwir-scratch/findings-results/Q33-tier-mismatch-explainer.csv' (HEADER, DELIMITER ',');
