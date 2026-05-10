-- Q29 — Alignment explainer table (5 hand-picked games across the alignment range)
-- Backs the lede table in sentiment-vote-alignment.md: one row per "what alignment looks like"
-- bucket (small gap near ceiling, mild rage-positive, deep rage-positive, disappointment-positive,
-- extreme rage-positive).
-- Output: G:/Work/Altanwir-scratch/findings-results/Q29-alignment-explainer.csv

COPY (
  WITH picks(gameName) AS (VALUES
    ('Stardew Valley'),
    ('Phasmophobia'),
    ('Sekiro: Shadows Die Twice'),
    ('Starfield'),
    ('Outlast')
  )
  SELECT
    fgs.gameName,
    fgs.weightedSentimentRating,
    fgs.weightedVoteRating,
    fgs.steamRatingLabel,
    fgs.sentimentVoteAlignment,
    fgs.totalReviews
  FROM gold.vw_factGameScores fgs
  JOIN picks p ON p.gameName = fgs.gameName
  ORDER BY fgs.sentimentVoteAlignment DESC
) TO 'G:/Work/Altanwir-scratch/findings-results/Q29-alignment-explainer.csv' (HEADER, DELIMITER ',');
